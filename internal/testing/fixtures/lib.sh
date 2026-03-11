#!/bin/bash

# ============================================
# Test Fixture Generation Library
# ============================================

# Check if IPFS is running
check_ipfs_running() {
  if ! ipfs id >/dev/null 2>&1; then
    echo "Error: IPFS daemon not running. Please start it first with 'ipfs daemon'"
    return 1
  fi
  return 0
}

# Check dependencies
check_dependencies() {
  if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed. Please install jq first."
    return 1
  fi
  return 0
}

# ============================================
# IPFS Operations
# ============================================

# Get block size from ipfs block stat output
get_block_size() {
  local CID="$1"
  if [ -z "$CID" ]; then
    echo "Error: Empty CID provided" >&2
    return 1
  fi
  
  local STATS
  STATS=$(ipfs block stat -- "$CID" 2>/dev/null)
  if [ $? -ne 0 ]; then
    echo "Error: Failed to get block stats for $CID" >&2
    return 1
  fi
  
  local SIZE
  SIZE=$(echo "$STATS" | grep 'Size:' | awk '{print $2}')
  if [ -z "$SIZE" ]; then
    echo "Error: Could not parse size from block stats" >&2
    return 1
  fi
  
  echo "$SIZE"
  return 0
}

# Calculate raw CID for a file
calculate_raw_cid() {
  local FILE="$1"
  if [ ! -e "$FILE" ]; then
    echo "Error: File '$FILE' does not exist" >&2
    return 1
  fi

  local max_retries=3
  local retry_delay=5
  local retry_count=0
  local OUTPUT

  while [ $retry_count -lt $max_retries ]; do
    if OUTPUT=$(ipfs dag put --store-codec=raw --input-codec=raw --allow-big-block "$FILE" 2>&1); then
      echo "$OUTPUT"
      return 0
    fi
    
    local filesize
    filesize=$(stat -c%s "$FILE")
    if [ "$filesize" -le 1048576 ]; then
      break
    fi

    echo "Warning: Failed to calculate raw CID for $FILE (attempt $((retry_count+1))/$max_retries)" >&2
    retry_count=$((retry_count+1))
    sleep $retry_delay
  done

  echo "Error: Failed to calculate raw CID for $FILE after $max_retries attempts" >&2
  echo "Output: $OUTPUT" >&2
  return 1
}

# Add a file to IPFS and return the CID
add_to_ipfs() {
  local FILE="$1"
  if [ ! -e "$FILE" ]; then
    echo "Error: File '$FILE' does not exist" >&2
    return 1
  fi
  
  local OUTPUT
  
  if [[ "$FILE" == *data_* ]]; then
    OUTPUT=$(calculate_raw_cid "$FILE")
  else
    OUTPUT=$(ipfs add -Q --pin=false "$FILE" 2>&1)
    if [ $? -ne 0 ]; then
      echo "Error: Failed to add file $FILE to IPFS: $OUTPUT" >&2
      return 1
    fi
  fi
  echo "$OUTPUT"
}

# Add a directory to IPFS and return the CID
add_directory_to_ipfs() {
  local DIR="$1"
  if [ ! -d "$DIR" ]; then
    echo "Error: Directory $DIR does not exist" >&2
    return 1
  fi
  local OUTPUT
  OUTPUT=$(ipfs add -Q -r --pin=false "$DIR" 2>/dev/null)
  if ! ipfs add -Q -r --pin=false "$DIR" >/dev/null 2>&1; then
    echo "Error: Failed to add directory $DIR to IPFS" >&2
    return 1
  fi
  echo "$OUTPUT"
}

# Export a block
export_block() {
  local CID="$1"
  local OUTPUT_FILE="$2"
  if [ -z "$CID" ]; then
    echo "Error: Empty CID provided" >&2
    return 1
  fi
  if ! ipfs block get "$CID" > "$OUTPUT_FILE" 2>/dev/null; then
    echo "Error: Failed to export block '$CID'" >&2
    return 1
  fi
  return 0
}

# Remove a block
remove_block() {
  local CID="$1"
  [ -z "$CID" ] && return 1
  echo "Removing block: $CID"
  
  if ! ipfs block rm "$CID" 2>/dev/null && ! ipfs dag rm "$CID" 2>/dev/null; then
    echo "Warning: Failed to remove block $CID (may not exist)"
    return 1
  fi
  return 0
}

# ============================================
# File/Directory Structure Operations
# ============================================

# Determine if a size is likely a chunk
is_likely_chunk() {
  local size=$1
  local threshold=245760
  local typical=262144
  if [[ "$size" -ge "$threshold" && "$size" -le "$typical" ]]; then
    echo "true"
  else
    echo "false"
  fi
}

# Create a file with specific content
create_file() {
  local FILE="$1"
  local SIZE="$2"
  local TYPE="$3"

  case "$TYPE" in
    txt|mp4)
      # Use deterministic content for tests
      if [ "$DETERMINISTIC" = "1" ]; then
        # Create repeated pattern for deterministic output
        local pattern="IPFS test data. "
        local pattern_len=${#pattern}
        local pattern_count=$((SIZE / pattern_len))
        for ((i=0; i<pattern_count; i++)); do
          echo -n "$pattern" >> "$FILE"
        done
        local remaining=$((SIZE % pattern_len))
        [ $remaining -gt 0 ] && echo -n "${pattern:0:$remaining}" >> "$FILE"
      else
        head -c "$SIZE" /dev/urandom > "$FILE"
      fi
      ;;
    *)  # Default to random data
      head -c "$SIZE" /dev/urandom > "$FILE"
      ;;
  esac
}

# Create a file with specific content (deterministic version)
create_file_deterministic() {
  local FILE="$1"
  local SIZE="$2"
  local TYPE="$3"
  DETERMINISTIC=1 create_file "$FILE" "$SIZE" "$TYPE"
}

# Create a UnixFS directory
create_directory() {
  local DIR="$1"
  mkdir -p "$DIR"
}

# Create deep nested structure
create_deep_structure() {
  local dir="$1"
  local depth="$2"
  mkdir -p "$dir"
  for i in $(seq 1 "$depth"); do
    dir="$dir/level$i"
    mkdir -p "$dir"
    create_file "$dir/file${i}_1.txt" $((1024 + i)) txt
    create_file "$dir/file${i}_2.txt" $((2048 + i)) txt
  done
}

# Generate many files
generate_many_files() {
  local dir="$1"
  local count="$2"
  mkdir -p "$dir"
  for i in $(seq 1 "$count"); do
    create_file "$dir/file${i}.txt" $((1024 + (i % 10) * 512)) txt &
    [ $((i % 10)) -eq 0 ] && wait # Batch in groups of 10
  done
  wait
}

# Generate mixed content
generate_mixed_content() {
  local dir="$1"
  mkdir -p "$dir"
  # Regular files
  create_file "$dir/regular1.txt" 1024 txt
  create_file "$dir/regular2.bin" 2048 txt
  # Subdirectories
  mkdir -p "$dir/sub1"
  create_file "$dir/sub1/file1.txt" 1024 txt
  mkdir -p "$dir/sub2"
  create_file "$dir/sub2/file2.txt" 2048 txt
  return 0
}

# ============================================
# Info File Operations
# ============================================

# Create standardized JSON info files
create_info_file() {
  local output_file="$1"
  shift
  
  if [[ -f "$output_file" && "$output_file" == *protobuf_* ]]; then
    return 0
  fi

  local jq_args=()
  local jq_query='{}'

  while [ $# -gt 0 ]; do
    case "$1" in
      --file)
        jq_query+=' | .file = $file'
        jq_args+=(--arg file "$2")
        shift 2
        ;;
      --dir)
        jq_query+=' | .dir = $dir'
        jq_args+=(--arg dir "$2")
        shift 2
        ;;
      --size)
        jq_query+=' | .size = ($size | tonumber)'
        jq_args+=(--arg size "$2")
        shift 2
        ;;
      --missing)
        jq_query+=' | .missing = ($missing | tonumber)'
        jq_args+=(--arg missing "$2")
        shift 2
        ;;
      --cid)
        jq_query+=' | .cid = $cid'
        jq_args+=(--arg cid "$2")
        shift 2
        ;;
      --is_partial)
        jq_query+=' | .is_partial = ($is_partial | test("true"))'
        jq_args+=(--arg is_partial "$2")
        shift 2
        ;;
      --type)
        jq_query+=' | .type = $type'
        jq_args+=(--arg type "$2")
        shift 2
        ;;
      --message_size)
        jq_query+=' | .message_size = ($message_size | tonumber)'
        jq_args+=(--arg message_size "$2")
        shift 2
        ;;
      --raw_block_size)
        jq_query+=' | .raw_block_size = ($raw_block_size | tonumber)'
        jq_args+=(--arg raw_block_size "$2")
        shift 2
        ;;
      *) shift ;;
      *) shift ;;
    esac
  done

  mkdir -p -- "$(dirname "$output_file")"
  
  if [[ -f "$output_file" ]]; then
    local new_json
    new_json=$(jq -n "${jq_args[@]}" "$jq_query") || return 1
    # Merge with existing file, but new values override old values
    jq -n --argjson new "$new_json" 'input as $old | $old * $new' "$output_file" > "${output_file}.tmp" && mv "${output_file}.tmp" "$output_file"
  else
    jq -n "${jq_args[@]}" "$jq_query" > "$output_file"
  fi
}

# ============================================
# Cleanup Functions
# ============================================

# Download and extract BBB video file
download_bbb_video() {
  local output_dir="$1"
  local bbb_file="$output_dir/bbb_sunflower_1080p_60fps_stereo_abl.mp4"
  local bbb_url="https://download.blender.org/demo/movies/BBB/bbb_sunflower_1080p_60fps_stereo_abl.mp4.zip"

  if [ -e "$bbb_file" ]; then
    echo "BBB video already exists at $bbb_file"
    return 0
  fi

  echo "Downloading BBB video from $bbb_url..."
  mkdir -p "$output_dir"

  # Create temporary directory for download
  local work_dir=$(mktemp -d)

  # Download the zip file
  if ! wget -q --timeout=300 --tries=3 -O "${work_dir}/bbb.zip" "$bbb_url"; then
    echo "Error: Failed to download BBB video" >&2
    rm -rf "$work_dir"
    return 1
  fi

  echo "Extracting BBB video..."
  # Extract directly to output directory
  if ! unzip -q -o "${work_dir}/bbb.zip" -d "$output_dir"; then
    echo "Error: Failed to extract BBB video" >&2
    rm -rf "$work_dir"
    return 1
  fi

  # Cleanup temporary files
  rm -rf "$work_dir"

  echo "BBB video downloaded and extracted successfully"
  return 0
}

# Cleanup handler
cleanup() {
  if [ -z "$CLEANUP_DONE" ]; then
    CLEANUP_DONE=1
    local exit_code=$?
    local TEMP_DIR="${1:-$TEMP_DIR}"
    echo -e "\nCleaning up temporary directory..."
    rm -rf "$TEMP_DIR"
    if [ $exit_code -ne 0 ]; then
      echo "Script failed with exit code $exit_code"
    fi
    exit $exit_code
  fi
}

# ============================================
# CAR File Generation Functions
# ============================================

# Generate CAR file from a source file
generate_car_from_file() {
  local source_file="$1"
  local output_file="$2"
  local name="$3"

  if [ ! -e "$source_file" ]; then
    echo "Error: Source file not found: $source_file" >&2
    return 1
  fi

  echo "Generating CAR for $name from $source_file..."

  # Add file to IPFS
  local CID
  CID=$(ipfs add -Q --pin=false "$source_file")
  if [ -z "$CID" ]; then
    echo "Error: Failed to add $source_file to IPFS" >&2
    return 1
  fi
  echo "  CID: $CID"

  # Export as CAR
  mkdir -p -- "$(dirname "$output_file")"
  ipfs dag export "$CID" > "$output_file"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to export CAR for $name" >&2
    return 1
  fi

  # Get file sizes
  local source_size
  source_size=$(stat -c%s "$source_file")
  local car_size
  car_size=$(stat -c%s "$output_file")

  echo "  Source size: $source_size bytes"
  echo "  CAR size: $car_size bytes"
  echo "  Output: $output_file"

  return 0
}

# Generate CAR file from a directory structure
generate_directory_car() {
  local template_dir="$1"
  local output_file="$2"
  local name="${3:-Directory Tree}"
  local cid_version="${4:-0}"  # Default to CIDv0 for backward compatibility

  if [ ! -d "$template_dir" ]; then
    echo "Error: Template directory not found: $template_dir" >&2
    return 1
  fi

  echo "Generating CAR for $name..."

  # Add directory to IPFS
  local CID
  CID=$(ipfs add -Q -r --pin=false --cid-version="${cid_version}" "$template_dir")
  if [ -z "$CID" ]; then
    echo "Error: Failed to add directory to IPFS" >&2
    return 1
  fi
  echo "  CID: $CID"

  # Export as CAR
  mkdir -p -- "$(dirname "$output_file")"
  ipfs dag export "$CID" > "$output_file"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to export CAR for $name" >&2
    return 1
  fi

  local car_size
  car_size=$(stat -c%s "$output_file")

  echo "  CAR size: $car_size bytes"
  echo "  Output: $output_file"

  return 0
}


