package main

import (
	"context"
	"multiImporter/importer"
)

func main() {
	importer, _ := importer.NewImporter("123")
	importer.Start(context.Background())
}
