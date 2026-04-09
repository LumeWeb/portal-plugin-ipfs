#!/bin/bash

# Generate CAR fixtures for portal-plugin-ipfs
# Uses ipfs-content's infrastructure, outputs to portal-plugin-ipfs's cars directory

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/cars"

# ============================================
# Helper function to find ipfs-content fixtures
# Tries all approaches like a Go version would
# ============================================
find_ipfs_content_fixtures() {
    local current_dir="$1"
    local max_depth=10
    local ipfs_content_module="go.lumeweb.com/ipfs-content"
    local test_data_dir="internal/testing/fixtures"

    # Approach 1: Try go list first (fastest method)
    local mod_dir
    mod_dir=$(go list -m -f '{{.Dir}}' go.lumeweb.com/ipfs-content 2>/dev/null)
    if [ -n "$mod_dir" ]; then
        local fixtures_dir="$mod_dir/$test_data_dir"
        if [ -f "$fixtures_dir/lib.sh" ]; then
            echo "$fixtures_dir"
            return 0
        fi
    fi

    # Approach 2: Fall back to vendor directory scanning (like Node.js)
    local check_dir="$current_dir"
    local depth=0

    while [ "$depth" -lt "$max_depth" ]; do
        # Check for vendor directory at current level
        local vendor_dir="$check_dir/vendor/$ipfs_content_module/$test_data_dir"
        if [ -f "$vendor_dir/lib.sh" ]; then
            echo "$vendor_dir"
            return 0
        fi

        # Move up one directory
        local parent_dir
        parent_dir=$(dirname "$check_dir")

        # Check if we've reached the root
        if [ "$parent_dir" = "$check_dir" ] || [ "$parent_dir" = "/" ]; then
            break
        fi

        check_dir="$parent_dir"
        depth=$((depth + 1))
    done

    # Approach 3: Try relative path (assuming sibling repos)
    local rel_path="$current_dir/../../$ipfs_content_module/$test_data_dir"
    if [ -f "$rel_path/lib.sh" ]; then
        echo "$rel_path"
        return 0
    fi

    # Approach 4: Last resort - try from current working directory
    local vendor_cwd="$(pwd)/vendor/$ipfs_content_module/$test_data_dir"
    if [ -f "$vendor_cwd/lib.sh" ]; then
        echo "$vendor_cwd"
        return 0
    fi

    # Not found
    echo "Error: Could not find ipfs-content fixtures using any approach:" >&2
    echo "  1. go list -m (failed)" >&2
    echo "  2. Vendor scanning (failed)" >&2
    echo "  3. Relative paths (failed)" >&2
    echo "  4. Current directory vendor (failed)" >&2
    return 1
}

# Find ipfs-content fixtures directory
FIXTURES_DIR=$(find_ipfs_content_fixtures "$SCRIPT_DIR")

if [ $? -ne 0 ]; then
    exit 1
fi

LIB_SH="$FIXTURES_DIR/lib.sh"

echo "✓ Found ipfs-content fixtures at: $FIXTURES_DIR"

# Source ipfs-content's lib.sh
source "$LIB_SH" || {
    echo "Error: Failed to source $LIB_SH" >&2
    exit 1
}

# Setup
DEFAULT_TEMP_DIR="$SCRIPT_DIR/tmp"
mkdir -p "$DEFAULT_TEMP_DIR"
TEMP_DIR=$(mktemp -d "$DEFAULT_TEMP_DIR/ipfs-test-car.XXXXXX")
echo "Generating CAR fixtures in: $TEMP_DIR"
mkdir -p -- "${OUTPUT_DIR}"

# Initialize cleanup flag
CLEANUP_DONE=""
trap cleanup EXIT

# Check dependencies
check_dependencies || exit 1
check_ipfs_running || exit 1

# Generate CAR fixtures (ipfs-content style)
echo -e "\n=== Generating CAR Fixtures ==="

# Generate docx.car from sia.docx
echo "=== DOCX CAR ==="
if [ -e "$SCRIPT_DIR/sia.docx" ]; then
  generate_car_from_file \
    "$SCRIPT_DIR/sia.docx" \
    "$OUTPUT_DIR/docx.car" \
    "DOCX"
  echo ""
fi

# Generate filetree.car
echo "=== File Tree CAR ==="
FILETREE_DIR="$TEMP_DIR/filetree_test"
mkdir -p "$FILETREE_DIR"

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

for i in $(seq 1 1200); do
  DETERMINISTIC=1 create_file "$HAMT_DIR/file_$(printf "%04d" $i).txt" 512 txt
done

generate_directory_car "$HAMT_DIR" "$OUTPUT_DIR/hamttree.car" "HAMT Tree" 1
echo ""

echo -e "\n=== Test data generation complete. ==="
echo "Output directory: $OUTPUT_DIR"
