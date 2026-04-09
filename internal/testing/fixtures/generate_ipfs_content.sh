#!/bin/bash

# Generate IPFS content fixtures by running go generate in ipfs-content
# This script discovers ipfs-content location and runs its go generation

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ============================================
# Helper function to find ipfs-content fixtures
# Tries all approaches like a Go version would
# ============================================
find_ipfs_content_fixtures() {
    local project_root="$1"
    local max_depth=10
    local ipfs_content_module="go.lumeweb.com/ipfs-content"
    local test_data_dir="internal/testing/fixtures"

    # Approach 1: Try go list from project root first (fastest method)
    # Use -mod=mod to bypass vendor and get actual module path
    local mod_dir
    mod_dir=$(cd "$project_root" && go list -mod=mod -m -f '{{.Dir}}' "$ipfs_content_module" 2>/dev/null)
    if [ -n "$mod_dir" ]; then
        local fixtures_dir="$mod_dir/$test_data_dir"
        if [ -f "$fixtures_dir/lib.sh" ]; then
            echo "$fixtures_dir"
            return 0
        fi
    fi

    # Approach 2: Fall back to vendor directory scanning (like Node.js)
    local check_dir="$project_root"
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
    local rel_path="$project_root/../../$ipfs_content_module/$test_data_dir"
    if [ -f "$rel_path/lib.sh" ]; then
        echo "$rel_path"
        return 0
    fi

    # Approach 4: Last resort - try from current working directory
    local vendor_cwd
    vendor_cwd="$(pwd)/vendor/$ipfs_content_module/$test_data_dir"
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
if ! FIXTURES_DIR=$(find_ipfs_content_fixtures "$PROJECT_ROOT"); then
    exit 1
fi

echo "✓ Found ipfs-content fixtures at: $FIXTURES_DIR"

# Run generation commands manually in ipfs-content (avoid go generate in dependency modules)
echo ""
echo "Running ipfs-content fixture generation..."
(cd "$FIXTURES_DIR" && bash ./generate_car.sh && bash ./generate_block.sh && go run ./invalid_car_generator.go && go run ./empty_car_generator.go) || {
    echo "Error: fixture generation failed in ipfs-content" >&2
    exit 1
}

echo ""
echo "✓ IPFS content fixtures generated successfully"
