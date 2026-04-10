#!/bin/bash

# Generate IPFS content fixtures by running go generate in ipfs-content
# This script discovers ipfs-content locations and runs its generation

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# ============================================
# Helper function to find ipfs-content fixtures directory
# For bash scripts (generate_car.sh, generate_block.sh)
# ============================================
find_ipfs_content_fixtures_dir() {
    local project_root="$1"
    local ipfs_content_module="go.lumeweb.com/ipfs-content"

    # Approach 1: Check vendor directory first (priority)
    local vendor_fixtures="$project_root/vendor/$ipfs_content_module/internal/testing/fixtures"
    if [ -f "$vendor_fixtures/lib.sh" ]; then
        echo "$vendor_fixtures"
        return 0
    fi

    # Approach 2: Try go list -mod=mod for fallback
    local mod_dir
    mod_dir=$(cd "$project_root" && go list -mod=mod -m -f '{{.Dir}}' "$ipfs_content_module" 2>/dev/null)
    if [ -n "$mod_dir" ]; then
        local mod_fixtures="$mod_dir/internal/testing/fixtures"
        if [ -f "$mod_fixtures/lib.sh" ]; then
            echo "$mod_fixtures"
            return 0
        fi
    fi

    # Approach 3: Try relative path (assuming sibling repos)
    local rel_fixtures="$project_root/../../$ipfs_content_module/internal/testing/fixtures"
    if [ -f "$rel_fixtures/lib.sh" ]; then
        echo -n "$(cd "$rel_fixtures" && pwd)"
        return 0
    fi

    # Not found
    echo "Error: Could not find ipfs-content fixtures directory using any approach:" >&2
    echo "  1. Vendor directory (failed)" >&2
    echo "  2. go list -mod=mod (failed)" >&2
    echo "  3. Relative paths (failed)" >&2
    return 1
}

# ============================================
# Helper function to find ipfs-content module directory
# For running Go apps via go run
# ============================================
find_ipfs_content_module_dir() {
    local project_root="$1"
    local ipfs_content_module="go.lumeweb.com/ipfs-content"

    # Use go list -mod=mod to find the actual module directory
    # (Go apps are typically not in vendor, so we use module cache)
    local mod_dir
    mod_dir=$(cd "$project_root" && go list -mod=mod -m -f '{{.Dir}}' "$ipfs_content_module" 2>/dev/null)
    if [ -n "$mod_dir" ] && [ -d "$mod_dir" ]; then
        echo "$mod_dir"
        return 0
    fi

    # Fallback: try relative path
    local rel_path="$project_root/../../$ipfs_content_module"
    if [ -d "$rel_path" ]; then
        echo -n "$(cd "$rel_path" && pwd)"
        return 0
    fi

    echo "Error: Could not find ipfs-content module directory for Go apps" >&2
    return 1
}

# Find fixtures directory (for bash scripts)
if ! FIXTURES_DIR=$(find_ipfs_content_fixtures_dir "$PROJECT_ROOT"); then
    exit 1
fi

echo "✓ Found ipfs-content fixtures at: $FIXTURES_DIR"

# Find module directory (for Go apps)
if ! MODULE_DIR=$(find_ipfs_content_module_dir "$PROJECT_ROOT"); then
    exit 1
fi

echo "✓ Found ipfs-content module at: $MODULE_DIR"
echo ""

# Run generation commands
echo "Running ipfs-content fixture generation..."

# 1. Generate CAR files (bash script from fixtures dir)
echo "=== Generating CAR fixtures ==="
(cd "$FIXTURES_DIR" && bash ./generate_car.sh) || {
    echo "Error: CAR generation failed" >&2
    exit 1
}

# 2. Generate block data (bash script from fixtures dir)
echo ""
echo "=== Generating block fixtures ==="
(cd "$FIXTURES_DIR" && bash ./generate_block.sh) || {
    echo "Error: Block generation failed" >&2
    exit 1
}

# 3. Run Go apps from module directory, passing fixtures dir as argument
echo ""
echo "=== Running Go fixture generators ==="

# Run empty CAR generator
echo "--- Generating empty.car ---"
(cd "$MODULE_DIR" && go run ./testing/fixtures/cmd/empty-car-generator "$FIXTURES_DIR") || {
    echo "Error: empty-car-generator failed" >&2
    exit 1
}

# Run invalid CAR generator
echo "--- Generating invalid.car ---"
(cd "$MODULE_DIR" && go run ./testing/fixtures/cmd/invalid-car-generator "$FIXTURES_DIR") || {
    echo "Error: invalid-car-generator failed" >&2
    exit 1
}

echo ""
echo "✓ IPFS content fixtures generated successfully"
