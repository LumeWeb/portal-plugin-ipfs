#!/bin/bash

# Handle debug flags properly
while [[ "$1" == -* ]]; do
  case "$1" in
    -x|-v) shift ;; # Skip debug flags
    *) break ;;
  esac
done

SCRIPT_DIR=$(dirname "$0")

# Check dependencies
if ! command -v jq &> /dev/null; then
  echo "Error: jq is required but not installed. Please install jq first."
  exit 1
fi

# --- Setup ---
DEFAULT_OUTPUT_DIR="$SCRIPT_DIR/data"
DEFAULT_TEMP_DIR="$SCRIPT_DIR/tmp"
# Use first non-flag argument as output dir or default
OUTPUT_DIR="${1:-$DEFAULT_OUTPUT_DIR}"
mkdir -p "$DEFAULT_TEMP_DIR"
TEMP_DIR=$(mktemp -d "$DEFAULT_TEMP_DIR/ipfs-test-data.XXXXXX")
echo "Generating test data in: $TEMP_DIR"
mkdir -p -- "${OUTPUT_DIR}"

# --- Test Parameters ---
FILE_SIZES="0 1024 240000 256000 262144 524288 1048576 10485760 104857600" # Up to 100MB
MISSING_BLOCKS="0 1" 
FILE_EXT="data"
UNIXFS_TYPES="file directory"
BLOCK_SIZES="262144 524288 1048576"
DEPTHS="1 3 5" # Directory depth levels
FILE_COUNTS="1 10 100" # Number of files for many-files test
MAGIC_DIR_MESSAGE_SIZE=2

# --- Functions ---

remove_block() {
  CID=$1
  [ -z "$CID" ] && return 1
  echo "Removing block: $CID"
  
  # Try both block rm and dag rm since we have mixed CID types
  if ! ipfs block rm "$CID" 2>/dev/null && ! ipfs dag rm "$CID" 2>/dev/null; then
    echo "Warning: Failed to remove block $CID (may not exist)"
    return 1
  fi
  return 0
}

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

# Cleanup handler
cleanup() {
  # Only run cleanup once
  if [ -z "$CLEANUP_DONE" ]; then
    CLEANUP_DONE=1
    local exit_code=$?
    echo -e "\nCleaning up temporary directory..."
    rm -rf "$TEMP_DIR"
    if [ $exit_code -ne 0 ]; then
      echo "Script failed with exit code $exit_code"
    fi
    exit $exit_code
  fi
}

# Initialize cleanup flag
CLEANUP_DONE=""

# Trap signals and errors - using EXIT only to avoid multiple triggers
trap cleanup EXIT

# Function to determine if a size is likely a chunk
is_likely_chunk() {
  local size=$1
  # 240 KiB min, 256 KiB standard chunk size
  local threshold=245760
  local typical=262144
  if [[ "$size" -ge "$threshold" && "$size" -le "$typical" ]]; then
    echo "true"
  else
    echo "false"
  fi
}

# Function to create a file with specific content
create_file() {
  FILE="$1"
  SIZE="$2"
  TYPE="$3"

  case "$TYPE" in
    txt)
      head -c "$SIZE" /dev/urandom > "$FILE"
      ;;
    jpg)
      # Create a dummy JPG (replace with a real JPG generator if needed)
      echo "Dummy JPG data" > "$FILE"
      head -c "$SIZE" /dev/urandom >> "$FILE"
      ;;
    png)
      # Create a dummy PNG (replace with a real PNG generator if needed)
      echo "Dummy PNG data" > "$FILE"
      head -c "$SIZE" /dev/urandom >> "$FILE"
      ;;
    mp3)
      # Create a dummy MP3 data
      echo "Dummy MP3 data" > "$FILE"
      head -c "$SIZE" /dev/urandom >> "$FILE"
      ;;
    mp4)
      # Create a dummy MP4 data
      echo "Dummy MP4 data" > "$FILE"
      head -c "$SIZE" /dev/urandom >> "$FILE"
      ;;
    json)
      # Create a dummy JSON
      echo '{"key": "value"}' > "$FILE"
      local pad_size=$((SIZE - $(wc -c < "$FILE")))
      head -c "$pad_size" /dev/urandom >> "$FILE" # Pad to size
      ;;
    csv)
      # Create a dummy CSV
      echo "header1,header2" > "$FILE"
      echo "value1,value2" >> "$FILE"
      local pad_size=$((SIZE - $(wc -c < "$FILE")))
      head -c "$pad_size" /dev/urandom >> "$FILE" # Pad to size
      ;;
    *)
      echo "Unsupported file type: $TYPE"
      exit 1
      ;;
  esac
}

# Function to create a UnixFS directory
create_directory() {
  DIR="$1"
  mkdir -p "$DIR"
}

# Function to get block size from ipfs block stat output
get_block_size() {
  CID="$1"
  if [ -z "$CID" ]; then
    echo "Error: Empty CID provided" >&2
    return 1
  fi
  
  # Get block stats and parse size from output
  STATS=$(ipfs block stat -- "$CID" 2>/dev/null)
  if [ $? -ne 0 ]; then
    echo "Error: Failed to get block stats for $CID" >&2
    return 1
  fi
  
  # Extract size line and parse number
  SIZE=$(echo "$STATS" | grep 'Size:' | awk '{print $2}')
  if [ -z "$SIZE" ]; then
    echo "Error: Could not parse size from block stats" >&2
    return 1
  fi
  
  echo "$SIZE"
  return 0
}

# Function to calculate raw CID for a file
calculate_raw_cid() {
  FILE="$1"
  if [ ! -e "$FILE" ]; then
    echo "Error: File '$FILE' does not exist" >&2
    return 1
  fi

  # Try multiple times for large files
  local max_retries=3
  local retry_delay=5
  local retry_count=0

  while [ $retry_count -lt $max_retries ]; do
    # Force raw data CID generation using dag put with modern syntax and allow big blocks
    OUTPUT=$(ipfs dag put --store-codec=raw --input-codec=raw --allow-big-block "$FILE" 2>&1)
    if ipfs dag put --store-codec=raw --input-codec=raw --allow-big-block "$FILE" >/dev/null 2>&1; then
      echo "$OUTPUT"
      return 0
    fi
    
    # Only retry for files over 1MB
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

# Function to add a file to IPFS and return the CID
add_to_ipfs() {
  FILE="$1"
  if [ ! -e "$FILE" ]; then
    echo "Error: File '$FILE' does not exist" >&2
    return 1
  fi
  
  # Use raw CID for data_* files
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

# Function to add a directory to IPFS and return the CID
add_directory_to_ipfs() {
  DIR="$1"
  if [ ! -d "$DIR" ]; then
    echo "Error: Directory $DIR does not exist" >&2
    return 1
  fi
  OUTPUT=$(ipfs add -Q -r --pin=false "$DIR" 2>/dev/null)
  if ! ipfs add -Q -r --pin=false "$DIR" >/dev/null 2>&1; then
    echo "Error: Failed to add directory $DIR to IPFS" >&2
    return 1
  fi
  echo "$OUTPUT"
}

# Function to export a block
export_block() {
  CID="$1"
  OUTPUT_FILE="$2"
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


# Helper function to create standardized JSON info files
create_info_file() {
  local output_file="$1"
  shift
  
  # Skip if file exists and is from protobuf generator
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
      --depth)
        jq_query+=' | .depth = ($depth | tonumber)'
        jq_args+=(--arg depth "$2")
        shift 2
        ;;
      --count)
        jq_query+=' | .count = ($count | tonumber)'
        jq_args+=(--arg count "$2")
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
      *)
        shift
        ;;
    esac
  done

  mkdir -p -- "$(dirname "$output_file")"
  
  # Create new JSON or merge with existing file
  if [[ -f "$output_file" ]]; then
    # First create the new JSON structure
    local new_json
    new_json=$(jq -n "${jq_args[@]}" "$jq_query") || return 1
    # Then safely merge with existing file
    jq -n --argjson new "$new_json" 'input as $old | $new * $old' "$output_file" > "${output_file}.tmp" && 
    mv "${output_file}.tmp" "$output_file"
  else
    jq -n "${jq_args[@]}" "$jq_query" > "$output_file"
  fi
}


# Check if IPFS is running
if ! ipfs id >/dev/null 2>&1; then
  echo "Error: IPFS daemon not running. Please start it first with 'ipfs daemon'"
  exit 1
fi

# --- Main Loop ---

echo -e "\n=== Generating Raw Data Tests ==="
for SIZE in $FILE_SIZES; do
  for MISSING in $MISSING_BLOCKS; do
    echo "Creating test file of size ${SIZE} bytes"
    FILE="$TEMP_DIR/data_${SIZE}_${MISSING}.${FILE_EXT}"
    create_file "$FILE" "$SIZE" "txt"
    
    if [ "$SIZE" -eq 0 ]; then
      echo "Created empty file"
      echo "File: $FILE, Size: $SIZE, Missing: $MISSING, CID: N/A" > "$OUTPUT_DIR/data_${SIZE}_${MISSING}.info"
      continue
    fi

    CID=$(add_to_ipfs "$FILE")
    if [ -z "$CID" ]; then
      echo "Error: Failed to generate CID for $FILE"
      continue
    fi

    # Ensure CIDv1 format
    CIDV1=$(ipfs cid format -v 1 -b base32 "$CID" 2>/dev/null || echo "$CID")

    BLOCK_DATA_FILE="$OUTPUT_DIR/data_${SIZE}_${MISSING}.block"
    if ! export_block "$CID" "$BLOCK_DATA_FILE"; then
      echo "Error: Failed to export block $CID"
      continue
    fi
    
    if [ "$MISSING" -eq 1 ]; then 
      remove_block "$CID"
    fi
    
    # Determine if the file is likely a chunk
    IS_PARTIAL=$(is_likely_chunk "$SIZE")

    echo "Generated: $FILE -> $CIDV1"
    # Create standardized info file
    create_info_file "${OUTPUT_DIR}/data_${SIZE}_${MISSING}.info.json" \
      --file "$FILE" \
      --size "$SIZE" \
      --raw_block_size "$SIZE" \
      --missing "$MISSING" \
      --cid "$CIDV1" \
      --is_partial "$IS_PARTIAL"
  done
done

echo -e "\n=== Generating Multi-block Files ==="
for BLOCK_SIZE in $BLOCK_SIZES; do
  FILE_SIZE=$((BLOCK_SIZE * 3))
  echo -n "Generating multiblock_${BLOCK_SIZE}.data (${FILE_SIZE} bytes)... "
  FILE="$TEMP_DIR/multiblock_${BLOCK_SIZE}.data"
  create_file "$FILE" "$FILE_SIZE" "txt"
  
  CID=$(add_to_ipfs "$FILE")
  [ -z "$CID" ] && { echo "failed"; continue; }
  
  BLOCK_DATA_FILE="${OUTPUT_DIR}/multiblock_${BLOCK_SIZE}.block"
  mkdir -p -- "${OUTPUT_DIR}"
  export_block "$CID" "$BLOCK_DATA_FILE" || { echo "export failed"; continue; }
  echo "$CID"
  create_info_file "$OUTPUT_DIR/multiblock_${BLOCK_SIZE}.info.json" \
    --file "$FILE" \
    --size "$FILE_SIZE" \
    --cid "$CID"
done

echo -e "\n=== Generating Deep Directory Structures ==="
for DEPTH in $DEPTHS; do
  echo -n "Generating deep_${DEPTH}... "
  DIR="$TEMP_DIR/deep_${DEPTH}"
  create_deep_structure "$DIR" "$DEPTH"
  
  CID=$(add_directory_to_ipfs "$DIR")
  [ -z "$CID" ] && { echo "failed"; continue; }
  
  BLOCK_DATA_FILE="$OUTPUT_DIR/deep_${DEPTH}.block"
  mkdir -p -- "${OUTPUT_DIR}"
  export_block "$CID" "$BLOCK_DATA_FILE" || { echo "export failed"; continue; }
  echo "$CID"
  create_info_file "$OUTPUT_DIR/deep_${DEPTH}.info.json" \
    --dir "$DIR" \
    --depth "$DEPTH" \
    --cid "$CID"
done

echo -e "\n=== Generating Many Files Tests ==="
for COUNT in $FILE_COUNTS; do
  echo -n "Generating many_files_${COUNT}... "
  DIR="$TEMP_DIR/many_files_${COUNT}"
  generate_many_files "$DIR" "$COUNT"
  
  CID=$(add_directory_to_ipfs "$DIR")
  [ -z "$CID" ] && { echo "failed"; continue; }
  
  BLOCK_DATA_FILE="$OUTPUT_DIR/many_files_${COUNT}.block"
  mkdir -p "$OUTPUT_DIR"
  export_block "$CID" "$BLOCK_DATA_FILE" || { echo "export failed"; continue; }
  echo "$CID"
  create_info_file "$OUTPUT_DIR/many_files_${COUNT}.info.json" \
    --dir "$DIR" \
    --count "$COUNT" \
    --cid "$CID"
done

echo -e "\n=== Generating Mixed Content Directory ==="
echo -n "Generating mixed_content... "
DIR="$TEMP_DIR/mixed_content"
generate_mixed_content "$DIR"

CID=$(add_directory_to_ipfs "$DIR")
[ -z "$CID" ] && { echo "failed"; exit 1; }

BLOCK_DATA_FILE="$OUTPUT_DIR/mixed_content.block"
mkdir -p "$OUTPUT_DIR"
export_block "$CID" "$BLOCK_DATA_FILE" || { echo "export failed"; exit 1; }
echo "$CID"
create_info_file "$OUTPUT_DIR/mixed_content.info.json" \
  --dir "$DIR" \
  --cid "$CID"

# --- Build Protobuf Generator ---
echo -e "\n=== Building Protobuf Generator ==="
echo "Building protobuf generator..."
go build -o "$TEMP_DIR/protobuf_generator" "$SCRIPT_DIR/protobuf_generator.go"

# --- Generate Protobuf Test Data ---
echo -e "\n=== Generating Protobuf Test Data ==="
for SIZE in $FILE_SIZES; do
  for MISSING in $MISSING_BLOCKS; do
    echo -e "\nGenerating protobuf data (size: ${SIZE}, missing: ${MISSING})..."
    
    # Generate protobuf data with output dir
    CID=$(OUTPUT_DIR="$OUTPUT_DIR" "$TEMP_DIR/protobuf_generator" -size "$SIZE" -partial "$MISSING")
    echo "Generated CID: $CID"
    
    # Export block
    BLOCK_DATA_FILE="${OUTPUT_DIR}/protobuf_${SIZE}_${MISSING}.block"
    mkdir -p -- "${OUTPUT_DIR}"
    ipfs block get "$CID" > "$BLOCK_DATA_FILE" || { echo "Failed to export block"; continue; }
    
    # Determine if the file is likely a chunk
    IS_PARTIAL=$(is_likely_chunk "$SIZE")

    # Protobuf generator already created the info file - don't overwrite
    # Just validate it exists
    info_file="${OUTPUT_DIR}/protobuf_${SIZE}_${MISSING}.info.json"
    if [[ ! -f "$info_file" ]]; then
    	echo "Error: Protobuf info file not created: $info_file"
    	exit 1
    fi

    # Remove block if testing missing case
    if [ "$MISSING" -eq 1 ]; then
      ipfs block rm "$CID" >/dev/null 2>&1 || true
    fi
  done
done

# Cleanup
rm "$TEMP_DIR/protobuf_generator"

# Existing UnixFS and Symlink tests follow...
for SIZE in $FILE_SIZES; do
  for MISSING in $MISSING_BLOCKS; do
    # Generate raw data file
    echo -n "Generating random data file (size: ${SIZE}, missing: ${MISSING})... "
    FILE="$TEMP_DIR/data_${SIZE}_${MISSING}.${FILE_EXT}"
    create_file "$FILE" "$SIZE" "txt" # Using txt type just for the random data generation
    echo "done"

    # Skip empty files (size 0) since they can't be added to IPFS
    if [ "$SIZE" -eq 0 ]; then
      echo "Skipping empty file: $FILE"
      echo -n "File: $FILE, Size: $SIZE, Missing Blocks: $MISSING, CID: N/A (empty file)" > "$OUTPUT_DIR/data_${SIZE}_${MISSING}.info"
      continue
    fi

    # Add to IPFS
    CID=$(add_to_ipfs "$FILE")
    [ -z "$CID" ] && { echo "Error: Failed to add non-empty file $FILE to IPFS"; continue; }

    # Define the output file for the block data
    BLOCK_DATA_FILE="$OUTPUT_DIR/data_${SIZE}_${MISSING}.block"

    # Export the block data
    if ! export_block "$CID" "$BLOCK_DATA_FILE"; then
      echo "Error: Failed to export block $CID"
      continue
    fi

    # Manipulate IPFS data (remove root block if MISSING=1)
    if [ "$MISSING" -eq 1 ]; then
      remove_block "$CID" # Remove the root block
    fi

    # Determine if the file is likely a chunk
    IS_PARTIAL=$(is_likely_chunk "$SIZE")

    CIDV1=$(ipfs cid format -v 1 -b base32 "$CID") # Convert to CIDv1

    # Output using standardized info file function
    create_info_file "$OUTPUT_DIR/data_${SIZE}_${MISSING}.info.json" \
      --file "$FILE" \
      --size "$SIZE" \
      --missing "$MISSING" \
      --cid "$CIDV1" \
      --is_partial "$IS_PARTIAL" \
      --raw_block_size "$SIZE"
  done
done

echo -e "\n=== Generating UnixFS Fixtures ==="
echo "Generating types: $UNIXFS_TYPES"

# UnixFS Tests (Directories and Files)
for MISSING in $MISSING_BLOCKS; do
  for UNIXFS_TYPE in $UNIXFS_TYPES; do
    echo -n "Generating UnixFS ${UNIXFS_TYPE} (missing: ${MISSING})... "
    # Create a directory structure
    DIR="$TEMP_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}"
    create_directory "$DIR"
    echo -n "created, "

    # Create files inside the directory based on the UnixFS type
    case "$UNIXFS_TYPE" in
      file)
        # Create a dummy file inside the directory
        DUMMY_FILE="$DIR/dummy_file.txt"
        create_file "$DUMMY_FILE" 1024 txt
        
        # Add just the file to IPFS to get proper UnixFS file structure
        FILE_CID=$(ipfs add -Q --pin=false "$DUMMY_FILE")
        [ -z "$FILE_CID" ] && { echo "Error: Failed to add file $DUMMY_FILE to IPFS"; continue; }
        
        # Export the file block which should be a proper UnixFS file node
        FILE_BLOCK_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}_file.block"
        if ! export_block "$FILE_CID" "$FILE_BLOCK_FILE"; then
            echo "Error: Failed to export file block $FILE_CID"
            continue
        fi

        # Also export the raw data block for the file contents
        RAW_CID=$(calculate_raw_cid "$DUMMY_FILE")
        [ -z "$RAW_CID" ] && { echo "Error: Failed to calculate raw CID for $DUMMY_FILE"; continue; }
        
        RAW_BLOCK_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.block"
        if ! export_block "$RAW_CID" "$RAW_BLOCK_FILE"; then
            echo "Error: Failed to export raw block $RAW_CID"
            continue
        fi
        ;;
      directory)
        # Create a subdirectory
        SUBDIR="$DIR/subdir"
        create_directory "$SUBDIR"
        # Create a dummy file inside the subdirectory
        DUMMY_FILE="$SUBDIR/dummy_file.txt"
        create_file "$DUMMY_FILE" 1024 txt

        # Add the directory to IPFS
        CID=$(add_directory_to_ipfs "$DIR")
        [ -z "$CID" ] && { echo "Error: Failed to add directory $DIR to IPFS"; continue; }

        # Export the block data
        BLOCK_DATA_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.block"
        if ! export_block "$CID" "$BLOCK_DATA_FILE"; then
          echo "Error: Failed to export block $CID"
          continue
        fi
        ;;
    esac

    # Determine if the file is likely a chunk
    is_partial="false"
    if [[ "$UNIXFS_TYPE" == "file" ]]; then
      is_partial=$(is_likely_chunk 1024)
    fi

    # Output
    if [[ "$UNIXFS_TYPE" == "file" ]]; then
      # Get block sizes using our parsing function
      RAW_BLOCK_SIZE=$(get_block_size "$RAW_CID")
      FILE_BLOCK_SIZE=$(get_block_size "$FILE_CID")

      # Info file for the raw data block
      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.info.json" \
        --file "$DUMMY_FILE" \
        --missing "$MISSING" \
        --cid "$RAW_CID" \
        --is_partial "$is_partial" \
        --type "raw_data" \
        --size "$RAW_BLOCK_SIZE" \
        --raw_block_size "$RAW_BLOCK_SIZE" \
        --message_size "$RAW_BLOCK_SIZE"
          
      # Info file for the UnixFS file block
      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}_file.info.json" \
        --file "$DUMMY_FILE" \
        --missing "$MISSING" \
        --cid "$FILE_CID" \
        --is_partial "$is_partial" \
        --type "unixfs_file" \
        --size "$FILE_BLOCK_SIZE" \
        --raw_block_size "$FILE_BLOCK_SIZE" \
        --message_size "$FILE_BLOCK_SIZE"

      # Remove blocks if testing missing case
      if [ "$MISSING" -eq 1 ]; then
        remove_block "$FILE_CID"
        remove_block "$RAW_CID"
      fi
    else
      # Get directory block size using our parsing function
      DIR_BLOCK_SIZE=$(get_block_size "$CID")

      # UnixFS directories have a magic message size of 2 bytes
      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.info.json" \
        --dir "$DIR" \
        --missing "$MISSING" \
        --cid "$CID" \
        --is_partial "$is_partial" \
        --type "$UNIXFS_TYPE" \
        --size "$DIR_BLOCK_SIZE" \
        --message_size "$MAGIC_DIR_MESSAGE_SIZE" \
        --raw_block_size "$DIR_BLOCK_SIZE"

      # Remove block if testing missing case
      if [ "$MISSING" -eq 1 ]; then
        remove_block "$CID"
      fi
    fi
  done
done


echo -e "\nTest data generation complete."
echo "Output directory: $OUTPUT_DIR"
exit 0
