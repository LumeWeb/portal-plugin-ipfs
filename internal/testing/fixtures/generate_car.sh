#!/bin/bash

# ============================================
# Test Fixtures Generation Script
# ============================================

# Handle debug flags properly
while [[ "$1" == -* ]]; do
  case "$1" in
    -x|-v) shift ;; # Skip debug flags
    *) break ;;
  esac
done

# Source library functions
SCRIPT_DIR=$(dirname "$0")
source "$SCRIPT_DIR/lib.sh" || exit 1

# --- Setup ---
DEFAULT_OUTPUT_DIR="$SCRIPT_DIR/cars"
DEFAULT_TEMP_DIR="$SCRIPT_DIR/tmp"
OUTPUT_DIR="${1:-$DEFAULT_OUTPUT_DIR}"
mkdir -p "$DEFAULT_TEMP_DIR"
TEMP_DIR=$(mktemp -d "$DEFAULT_TEMP_DIR/ipfs-test-data.XXXXXX")
echo "Generating test data in: $TEMP_DIR"
mkdir -p -- "${OUTPUT_DIR}"

# Initialize cleanup flag
CLEANUP_DONE=""
trap cleanup EXIT

# Check dependencies
check_dependencies || exit 1
check_ipfs_running || exit 1

# ============================================
# Fixtures Generation
# ============================================

echo -e "\n=== Generating CAR Fixtures ==="

# Download BBB video if needed
echo "=== Downloading BBB Video ==="
download_bbb_video "$SCRIPT_DIR" || exit 1
echo ""

# Generate bbb.car from Big Buck Bunny video
echo "=== Big Buck Bunny CAR ==="
if [ -e "$SCRIPT_DIR/bbb_sunflower_1080p_60fps_stereo_abl.mp4" ]; then
  generate_car_from_file \
    "$SCRIPT_DIR/bbb_sunflower_1080p_60fps_stereo_abl.mp4" \
    "$OUTPUT_DIR/bbb.car" \
    "Big Buck Bunny"
  echo ""
else
  echo "Warning: BBB video file not found, skipping" >&2
fi

# Generate docx.car from sia.docx
echo "=== DOCX CAR ==="
if [ -e "$SCRIPT_DIR/sia.docx" ]; then
  generate_car_from_file \
    "$SCRIPT_DIR/sia.docx" \
    "$OUTPUT_DIR/docx.car" \
    "SIA DOCX"
  echo ""
else
  echo "Warning: sia.docx not found, skipping" >&2
fi

# Generate filetree.car from a structured directory
echo "=== File Tree CAR ==="
FILETREE_DIR="$TEMP_DIR/filetree_test"
mkdir -p "$FILETREE_DIR"

# Create deterministic nested structure
for i in $(seq 1 20); do
  DIR="$FILETREE_DIR/dir_$(printf "%02d" $i)"
  mkdir -p "$DIR"
  DETERMINISTIC=1 create_file "$DIR/file_$(printf "%02d" $i).txt" $((1024 * (1 + (i % 10)))) txt
  for j in $(seq 1 5); do
    DETERMINISTIC=1 create_file "$DIR/subfile_$(printf "%02d" $j).txt" $((512 * j)) txt
  done
done

generate_directory_car "$FILETREE_DIR" "$OUTPUT_DIR/filetree.car" "File Tree"
echo ""

# Generate HAMT tree
echo "=== HAMT Tree CAR ==="
HAMT_DIR="$TEMP_DIR/hamt_test"
mkdir -p "$HAMT_DIR"

# Create files for HAMT tree
for i in $(seq 1 1200); do
  DETERMINISTIC=1 create_file "$HAMT_DIR/file_$(printf "%04d" $i).txt" 512 txt
done

# Save current settings and enable HAMT
OLD_THRESHOLD=$(ipfs config Import.UnixFSHAMTDirectorySizeThreshold 2>/dev/null || echo "")
OLD_CID_VERSION=$(ipfs config Import.CidVersion 2>/dev/null || echo "")
ipfs config Import.UnixFSHAMTDirectorySizeThreshold 0 >/dev/null 2>&1
ipfs config Import.CidVersion 1 >/dev/null 2>&1

generate_directory_car "$HAMT_DIR" "$OUTPUT_DIR/hamttree.car" "HAMT Tree" 1

# Restore settings
[ -n "$OLD_THRESHOLD" ] && ipfs config Import.UnixFSHAMTDirectorySizeThreshold "$OLD_THRESHOLD" >/dev/null 2>&1
[ -n "$OLD_CID_VERSION" ] && ipfs config Import.CidVersion "$OLD_CID_VERSION" >/dev/null 2>&1
echo ""

echo -e "\n=== Test data generation complete. ==="
echo "Output directory: $OUTPUT_DIR"
