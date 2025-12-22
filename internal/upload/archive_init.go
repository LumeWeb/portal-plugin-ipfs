package upload

// init registers built-in archive extractors with the default registry
func init() {
	// Register ZIP extractor
	RegisterZipExtractor()

	// Register TAR extractors
	RegisterTarExtractor()
	RegisterTarGzExtractor()
	RegisterTarBz2Extractor()

	// Register RAR extractor
	RegisterRarExtractor()

	// Register 7Z extractor
	Register7ZipExtractor()
}
