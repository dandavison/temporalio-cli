package common

import (
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
)

func LogToFile(msg string, prefix string, colorName string) {
	color.NoColor = false
	var colorObj color.Attribute
	switch colorName {
	case "red":
		colorObj = color.FgRed
	case "green":
		colorObj = color.FgGreen
	case "blue":
		colorObj = color.FgBlue
	default:
		colorObj = color.FgBlack
	}
	colorFn := color.New(colorObj).SprintfFunc()
	file, err := os.OpenFile("/tmp/log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	if _, err := file.WriteString(colorFn("%s %s: %s\n", time.Now().Format("15:04:05.000"), prefix, msg)); err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	if err := file.Sync(); err != nil {
		fmt.Println("Error flushing file:", err)
	}
}
