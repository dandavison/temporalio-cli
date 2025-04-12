package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
)

// --- Main Application Logic ---

func main() {
	// Logger initialization needs to happen early
	zapLogger, err := zap.NewDevelopment() // Or zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer zapLogger.Sync() // Flushes buffer, if any
	logger := zapLogger.Sugar()

	// Create and execute the command
	rootCmd := buildCliImageCmd(logger) // Pass logger to command builder
	if err := rootCmd.Execute(); err != nil {
		// Cobra automatically prints errors, but we log fatally just in case
		// and to ensure non-zero exit code if Execute doesn't handle it.
		logger.Fatalf("Command execution failed: %v", err)
		// os.Exit(1) // Fatalf already exits
	}
}

// buildCliImageCmd sets up the cobra command structure
func buildCliImageCmd(logger *zap.SugaredLogger) *cobra.Command { // Accept logger
	var b cliImageBuilder
	b.logger = logger // Assign logger to the builder instance

	cmd := &cobra.Command{
		Use:   "build-docker-image", // This will be the executable name if built directly
		Short: "Build Temporal CLI Docker image",
		Run: func(cmd *cobra.Command, args []string) {
			// The actual build logic is called here
			// Logger is already initialized and assigned to b.logger
			if err := b.build(cmd.Context()); err != nil {
				// Log the error using the already initialized logger
				// Use Errorf because Fatalf would exit(1) prematurely if Execute is supposed to handle exit codes.
				// Cobra's default behavior usually prints the error from RunE returning an error.
				b.logger.Errorf("Build failed: %v", err)
				os.Exit(1) // Explicitly exit non-zero on build error
			}
			b.logger.Info("Build successful.")
		},
		// Silence errors/usage because we handle logging in Run/main
		SilenceErrors: true,
		SilenceUsage:  true,
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
	gitRef, err := gitRef(".git") // Use helper defined below
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
	dockerArgs = append(dockerArgs, rootDir()) // Use helper defined below

	b.logger.Infof("Running docker command: docker %v", strings.Join(dockerArgs, " "))
	if b.dryRun {
		b.logger.Info("Dry run enabled, skipping execution.")
		return nil
	}

	// --- Execute Build ---
	// Write image tags to GitHub env if needed
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		err = writeGitHubEnv("FEATURES_BUILT_IMAGE_TAGS", strings.Join(imageTagsForPublish, ";")) // Use helper defined below
		if err != nil {
			// Log warning, don't fail the build just for this
			b.logger.Warnf("Writing image tags to github env failed: %v", err)
		}
	}

	// Use CommandContext for better control over execution and potential cancellation
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
			err = writeGitHubEnv("SAVED_IMAGE_TAG", imageNameAndVersionTag) // Use helper defined below
			if err != nil {
				b.logger.Warnf("Writing saved image tag to github env failed: %v", err)
			}
		}

		// Use CommandContext for saving as well
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

// --- Helper Functions (copied back for standalone script) ---

const errFileCmdFmt = "failed to write to github file: %v"

// writeGitHubEnv sets a GitHub environment value. Only works with values without a linebreak.
func writeGitHubEnv(name string, value string) (retErr error) {
	filepath := os.Getenv("GITHUB_ENV")
	if filepath == "" {
		// Just don't do anything if we're not running in a GH env
		return nil
	}
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		retErr = fmt.Errorf(errFileCmdFmt, err)
		return
	}

	defer func() {
		if err := f.Close(); err != nil && retErr == nil {
			// Assign error to the named return variable
			retErr = fmt.Errorf("failed closing github file: %w", err)
		}
	}()

	msg := []byte(fmt.Sprintf("%s=%s\n", name, value))
	if _, err := f.Write(msg); err != nil {
		retErr = fmt.Errorf(errFileCmdFmt, err)
		return
	}
	return // Use named return
}

// gitRef gets the current commit hash (long).
func gitRef(gitDir string) (string, error) { // Removed context argument
	// Ensure the .git directory path is correct relative to where the command runs
	if !filepath.IsAbs(gitDir) {
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("failed to get working directory: %w", err)
		}
		gitDir = filepath.Join(wd, gitDir)
	}

	// Check if .git directory exists before running git command
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Warning: git directory %q not found: %v\n", gitDir, err)
		return "unknown", nil // Return placeholder instead of erroring
	}

	cmd := exec.Command("git", "--git-dir", gitDir, "rev-parse", "HEAD") // Removed context
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		// Try getting the short ref as a fallback for shallow clones in CI
		cmdShort := exec.Command("git", "--git-dir", gitDir, "rev-parse", "--short", "HEAD") // Removed context
		cmdShort.Stderr = &stderr                                                            // Reuse stderr builder
		outShort, errShort := cmdShort.Output()
		if errShort == nil {
			fmt.Fprintf(os.Stderr, "Warning: could not get full git ref, using short ref: %v (stderr: %q)\n", err, stderr.String())
			return strings.TrimSpace(string(outShort)), nil
		}
		// If both failed, return the original error
		return "", fmt.Errorf("failed getting git ref from %s (stderr: %q): %w", gitDir, stderr.String(), err)
	}
	return strings.TrimSpace(string(out)), nil
}

// rootDir calculates the repository root based on the location of this file.
func rootDir() string {
	_, currFile, _, ok := runtime.Caller(0)
	if !ok {
		// Fallback if Caller info isn't available
		fmt.Fprintln(os.Stderr, "Warning: Could not determine caller information for rootDir, falling back to '.'")
		return "."
	}
	// Assumes this file is in '<root>/cmd/' or similar, go up one/two levels
	// Adjust if the script location changes relative to the root
	return filepath.Dir(filepath.Dir(currFile)) // Go up two levels from cmd/build_image.go to get repo root
}
