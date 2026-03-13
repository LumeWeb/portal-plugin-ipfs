package upload

import (
	"testing"
)

func TestFormatIsArchiveFormat(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		want   bool
	}{
		{
			name:   "UNKNOWN format is not archive",
			format: FormatUnknown,
			want:   false,
		},
		{
			name:   "CAR format is not archive",
			format: FormatCAR,
			want:   false,
		},
		{
			name:   "FILE format is not archive",
			format: FormatFile,
			want:   false,
		},
		{
			name:   "ZIP format is archive",
			format: FormatZIP,
			want:   true,
		},
		{
			name:   "RAR format is archive",
			format: FormatRAR,
			want:   true,
		},
		{
			name:   "TAR format is archive",
			format: FormatTAR,
			want:   true,
		},
		{
			name:   "TAR.GZ format is archive",
			format: FormatTAR_GZ,
			want:   true,
		},
		{
			name:   "TAR.BZ2 format is archive",
			format: FormatTAR_BZ2,
			want:   true,
		},
		{
			name:   "7Z format is archive",
			format: Format7Z,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.format.IsArchiveFormat(); got != tt.want {
				t.Errorf("Format.IsArchiveFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatIsUploadFormat(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		want   bool
	}{
		{
			name:   "UNKNOWN format is not upload",
			format: FormatUnknown,
			want:   false,
		},
		{
			name:   "CAR format is upload",
			format: FormatCAR,
			want:   true,
		},
		{
			name:   "FILE format is not upload",
			format: FormatFile,
			want:   false,
		},
		{
			name:   "ZIP format is not upload",
			format: FormatZIP,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.format.IsUploadFormat(); got != tt.want {
				t.Errorf("Format.IsUploadFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}
