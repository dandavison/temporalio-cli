package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
)

// Removed rootDir stub - uses definition from github.go

// Removed writeGitHubEnv stub - uses definition from github.go

func BuildCliImageCmd() *cobra.Command { // Exported function name
	var b cliImageBuilder
	cmd := &cobra.Command{
		Use:   "build-docker-image",
		Short: "Build Temporal CLI Docker image",
		Run: func(cmd *cobra.Command, args []string) {
			// Initialize logger here before calling build
			var err error
			var zapLogger *zap.Logger
			// Basic Zap logger configuration
			zapLogger, err = zap.NewDevelopment() // Or zap.NewProduction()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
				os.Exit(1)
			}
			defer zapLogger.Sync() // Flushes buffer, if any
			b.logger = zapLogger.Sugar()

			if err := b.build(cmd.Context()); err != nil {
				b.logger.Fatalf("Build failed: %v", err) // Use Fatalf for logging + exit(1)
			}
			b.logger.Info("Build successful.")
		},
	}
	b.addCLIFlags(cmd.Flags())
	cmd.MarkFlagRequired("version")
	return cmd
}

type cliImageBuilder struct {
	logger    *zap.SugaredLogger // Use zap directly for logging
	version   string
	platform  string
	imageName string
	dryRun    bool
	saveImage string
	tags      []string
	labels    []string
}

func (b *cliImageBuilder) addCLIFlags(fs *pflag.FlagSet) {
	fs.StringVar(&b.version, "version", "",
		"Temporal CLI version to build the image for (e.g., 1.21.0).")
	fs.StringVar(&b.platform, "platform", "", "Platform for use in docker build --platform (e.g., linux/amd64).")
	fs.StringVar(&b.imageName, "image-name", "temporalio/cli", "Name of the image repository (e.g., temporalio/cli).")
	fs.BoolVar(&b.dryRun, "dry-run", false, "If set, just print the commands that would run but do not run them.")
	fs.StringSliceVar(&b.tags, "image-tag", nil, "Additional tags to add to the image (e.g., 'latest'). Version tag is added automatically.")
	fs.StringSliceVar(&b.labels, "image-label", nil, "Additional labels to add to the image (e.g., 'maintainer=Temporal Team <team@temporal.io>'). Standard OCI/Temporal labels are added automatically.")
	fs.StringVar(&b.saveImage, "save-image", "", "If set, runs `docker save` on the produced image, saving it to the provided path.")
}

func (b *cliImageBuilder) build(ctx context.Context) error {
	// Logger should be initialized before calling build

	// Validate version format
	versionToCheck := b.version
	if !strings.HasPrefix(b.version, "v") {
		versionToCheck = "v" + versionToCheck
	}
	if semver.Canonical(versionToCheck) == "" {
		return fmt.Errorf("expected valid semver for version, got %q", b.version)
	}
	// Add the primary version tag
	b.tags = append([]string{b.version}, b.tags...) // Prepend version tag

	// --- Setup Standard Labels ---
	gitRef, err := gitRef(ctx, ".git")
	if err != nil {
		// Log warning instead of failing if git ref cannot be determined (e.g., not in git repo)
		b.logger.Warnf("Could not determine git revision: %v. Proceeding without revision label.", err)
		gitRef = "unknown" // Set a placeholder
		// Reset err to nil so we don't return prematurely
		err = nil
	}

	// Add standard labels if they weren't provided via --image-label
	b.addLabelIfNotPresent("org.opencontainers.image.created", time.Now().UTC().Format(time.RFC3339))
	b.addLabelIfNotPresent("org.opencontainers.image.source", "https://github.com/temporalio/cli")
	b.addLabelIfNotPresent("org.opencontainers.image.vendor", "Temporal Technologies Inc.")
	b.addLabelIfNotPresent("org.opencontainers.image.authors", "Temporal Maintainers <cli@temporal.io>") // Updated author
	b.addLabelIfNotPresent("org.opencontainers.image.licenses", "MIT")
	if gitRef != "unknown" {
		b.addLabelIfNotPresent("org.opencontainers.image.revision", gitRef)
	}
	b.addLabelIfNotPresent("org.opencontainers.image.title", "Temporal CLI")
	b.addLabelIfNotPresent("org.opencontainers.image.documentation", "https://github.com/temporalio/cli/blob/main/README.md") // Point to specific README
	b.addLabelIfNotPresent("org.opencontainers.image.version", b.version)                                                     // OCI standard version label
	b.addLabelIfNotPresent("io.temporal.cli.version", b.version)                                                              // Temporal-specific version label

	// --- Prepare docker command args ---
	dockerArgs := []string{
		"build",
		"--pull",                // Ensure base image is up-to-date
		"--file", ".Dockerfile", // Assumes Dockerfile is at the root
	}
	if b.platform != "" {
		dockerArgs = append(dockerArgs, "--platform", b.platform)
		// Pass platform as build-arg ONLY if Dockerfile uses it
		// dockerArgs = append(dockerArgs, "--build-arg", "PLATFORM="+b.platform)
	}

	var imageTagsForPublish []string
	imageNameAndVersionTag := "" // Track the specific version tag for saving
	for _, tag := range b.tags {
		tagVal := fmt.Sprintf("%s:%s", b.imageName, tag)
		dockerArgs = append(dockerArgs, "--tag", tagVal)
		imageTagsForPublish = append(imageTagsForPublish, tagVal)
		if tag == b.version {
			imageNameAndVersionTag = tagVal
		}
	}
	// Ensure we captured the primary tag for saving
	if imageNameAndVersionTag == "" && len(imageTagsForPublish) > 0 {
		imageNameAndVersionTag = imageTagsForPublish[0] // Fallback if version tag wasn't explicitly first
	} else if imageNameAndVersionTag == "" {
		return fmt.Errorf("no image tags were generated, cannot proceed")
	}

	for _, label := range b.labels {
		dockerArgs = append(dockerArgs, "--label", label)
	}
	// Add build args here if needed by .Dockerfile
	// for _, arg := range buildArgs {
	// 	dockerArgs = append(dockerArgs, "--build-arg", arg)
	// }

	// Add build context (the repository root)
	dockerArgs = append(dockerArgs, rootDir())

	b.logger.Infof("Running docker command: docker %v", strings.Join(dockerArgs, " "))
	if b.dryRun {
		b.logger.Info("Dry run enabled, skipping execution.")
		return nil
	}

	// --- Execute Build ---
	// Write image tags to GitHub env if needed
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		err = writeGitHubEnv("FEATURES_BUILT_IMAGE_TAGS", strings.Join(imageTagsForPublish, ";"))
		if err != nil {
			// Log warning, don't fail the build just for this
			b.logger.Warnf("Writing image tags to github env failed: %v", err)
		}
	}

	cmd := exec.CommandContext(ctx, "docker", dockerArgs...)
	cmd.Stdout = os.Stdout // Pipe docker build output directly
	cmd.Stderr = os.Stderr
	b.logger.Info("Starting Docker build...")
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed building image: %w", err)
	}
	b.logger.Info("Docker build completed.")

	// --- Save Image If Requested ---
	if b.saveImage != "" {
		b.logger.Infof("Saving image %s to %s", imageNameAndVersionTag, b.saveImage)
		// Write the saved image tag to GitHub env if needed
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			err = writeGitHubEnv("SAVED_IMAGE_TAG", imageNameAndVersionTag)
			if err != nil {
				b.logger.Warnf("Writing saved image tag to github env failed: %v", err)
			}
		}

		saveCmd := exec.CommandContext(ctx, "docker", "save", "-o", b.saveImage, imageNameAndVersionTag)
		saveCmd.Stdout = os.Stdout // Show output of save command
		saveCmd.Stderr = os.Stderr
		err = saveCmd.Run()
		if err != nil {
			return fmt.Errorf("failed saving image %s to %s: %w", imageNameAndVersionTag, b.saveImage, err)
		}
		b.logger.Infof("Successfully saved image to %s", b.saveImage)
	}

	return nil // Success
}

// addLabelIfNotPresent ensures a label with the given key doesn't already exist
// before adding the new key=value pair.
func (b *cliImageBuilder) addLabelIfNotPresent(key, value string) {
	prefix := key + "="
	for _, label := range b.labels {
		if strings.HasPrefix(label, prefix) {
			return // Label already exists
		}
	}
	b.labels = append(b.labels, prefix+value)
}

// Removed gitRef stub - uses definition from github.go
