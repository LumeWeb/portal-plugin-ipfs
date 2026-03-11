#!/bin/bash

# ============================================
# Test Fixtures Generation Script - Block Data
# Generates raw block fixtures for node tests
# ============================================

# Handle debug flags properly
while [[ "$1" == -* ]]; do
  case "$1" in
    -x|-v) shift ;;
    *) break ;;
  esac
done

# Source library functions
SCRIPT_DIR=$(dirname "$0")
source "$SCRIPT_DIR/lib.sh" || exit 1

# --- Setup ---
DEFAULT_OUTPUT_DIR="$SCRIPT_DIR/data"
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

# --- Test Parameters ---
FILE_SIZES="0 1024 240000 256000 262144 524288 1048576 10485760 104857600"
MISSING_BLOCKS="0 1"
FILE_EXT="data"
UNIXFS_TYPES="file directory"
BLOCK_SIZES="262144 524288 1048576"
DEPTHS="1 3 5"
FILE_COUNTS="1 10 100"
MAGIC_DIR_MESSAGE_SIZE=2

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

    CIDV1=$(ipfs cid format -v 1 -b base32 "$CID" 2>/dev/null || echo "$CID")

    BLOCK_DATA_FILE="$OUTPUT_DIR/data_${SIZE}_${MISSING}.block"
    if ! export_block "$CID" "$BLOCK_DATA_FILE"; then
      echo "Error: Failed to export block $CID"
      continue
    fi
    
    if [ "$MISSING" -eq 1 ]; then 
      remove_block "$CID"
    fi
    
    IS_PARTIAL=$(is_likely_chunk "$SIZE")

    echo "Generated: $FILE -> $CIDV1"
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

echo -e "\n=== Generating UnixFS Fixtures ==="
echo "Generating types: $UNIXFS_TYPES"

for MISSING in $MISSING_BLOCKS; do
  for UNIXFS_TYPE in $UNIXFS_TYPES; do
    echo -n "Generating UnixFS ${UNIXFS_TYPE} (missing: ${MISSING})... "
    DIR="$TEMP_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}"
    create_directory "$DIR"
    echo -n "created, "

    case "$UNIXFS_TYPE" in
      file)
        DUMMY_FILE="$DIR/dummy_file.txt"
        create_file "$DUMMY_FILE" 1024 txt
        
        FILE_CID=$(ipfs add -Q --pin=false "$DUMMY_FILE")
        [ -z "$FILE_CID" ] && { echo "Error: Failed to add file $DUMMY_FILE to IPFS"; continue; }
        
        FILE_BLOCK_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}_file.block"
        if ! export_block "$FILE_CID" "$FILE_BLOCK_FILE"; then
            echo "Error: Failed to export file block $FILE_CID"
            continue
        fi

        RAW_CID=$(calculate_raw_cid "$DUMMY_FILE")
        [ -z "$RAW_CID" ] && { echo "Error: Failed to calculate raw CID for $DUMMY_FILE"; continue; }
        
        RAW_BLOCK_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.block"
        if ! export_block "$RAW_CID" "$RAW_BLOCK_FILE"; then
            echo "Error: Failed to export raw block $RAW_CID"
            continue
        fi
        ;;
      directory)
        SUBDIR="$DIR/subdir"
        create_directory "$SUBDIR"
        DUMMY_FILE="$SUBDIR/dummy_file.txt"
        create_file "$DUMMY_FILE" 1024 txt

        CID=$(add_directory_to_ipfs "$DIR")
        [ -z "$CID" ] && { echo "Error: Failed to add directory $DIR to IPFS"; continue; }

        BLOCK_DATA_FILE="$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.block"
        if ! export_block "$CID" "$BLOCK_DATA_FILE"; then
          echo "Error: Failed to export block $CID"
          continue
        fi
        ;;
    esac

    is_partial="false"
    if [[ "$UNIXFS_TYPE" == "file" ]]; then
      is_partial=$(is_likely_chunk 1024)
    fi

    if [[ "$UNIXFS_TYPE" == "file" ]]; then
      RAW_BLOCK_SIZE=$(get_block_size "$RAW_CID")
      FILE_BLOCK_SIZE=$(get_block_size "$FILE_CID")

      # Convert CIDs to v1 for consistency
      RAW_CID_V1=$(ipfs cid format -v 1 -b base32 "$RAW_CID" 2>/dev/null || echo "$RAW_CID")
      FILE_CID_V1=$(ipfs cid format -v 1 -b base32 "$FILE_CID" 2>/dev/null || echo "$FILE_CID")

      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.info.json" \
        --file "$DUMMY_FILE" \
        --missing "$MISSING" \
        --cid "$RAW_CID_V1" \
        --is_partial "$is_partial" \
        --type "raw_data" \
        --size "$RAW_BLOCK_SIZE" \
        --raw_block_size "$RAW_BLOCK_SIZE" \
        --message_size "$RAW_BLOCK_SIZE"
          
      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}_file.info.json" \
        --file "$DUMMY_FILE" \
        --missing "$MISSING" \
        --cid "$FILE_CID_V1" \
        --is_partial "$is_partial" \
        --type "unixfs_file" \
        --size "$FILE_BLOCK_SIZE" \
        --raw_block_size "$FILE_BLOCK_SIZE" \
        --message_size "$FILE_BLOCK_SIZE"

      if [ "$MISSING" -eq 1 ]; then
        remove_block "$FILE_CID"
        remove_block "$RAW_CID"
      fi
    else
      DIR_BLOCK_SIZE=$(get_block_size "$CID")

      # Convert CID to v1 for consistency
      DIR_CID_V1=$(ipfs cid format -v 1 -b base32 "$CID" 2>/dev/null || echo "$CID")

      create_info_file "$OUTPUT_DIR/unixfs_${UNIXFS_TYPE}_${MISSING}.info.json" \
        --dir "$DIR" \
        --missing "$MISSING" \
        --cid "$DIR_CID_V1" \
        --is_partial "$is_partial" \
        --type "$UNIXFS_TYPE" \
        --size "$DIR_BLOCK_SIZE" \
        --message_size "$MAGIC_DIR_MESSAGE_SIZE" \
        --raw_block_size "$DIR_BLOCK_SIZE"

      if [ "$MISSING" -eq 1 ]; then
        remove_block "$CID"
      fi
    fi
  done
done

# --- Build Protobuf Generator ---
echo -e "\n=== Building Protobuf Generator ==="
echo "Building protobuf generator..."
go build -o "$TEMP_DIR/protobuf_generator" "$SCRIPT_DIR/protobuf_generator.go"

# --- Generate Protobuf Test Data ---
echo -e "\n=== Generating Protobuf Test Data ==="
for SIZE in $FILE_SIZES; do
  for MISSING in $MISSING_BLOCKS; do
    echo -e "\nGenerating protobuf data (size: ${SIZE}, missing: ${MISSING})..."
    
    CID=$(OUTPUT_DIR="$OUTPUT_DIR" "$TEMP_DIR/protobuf_generator" -size "$SIZE" -partial "$MISSING")
    echo "Generated CID: $CID"
    
    BLOCK_DATA_FILE="${OUTPUT_DIR}/protobuf_${SIZE}_${MISSING}.block"
    mkdir -p -- "${OUTPUT_DIR}"
    ipfs block get "$CID" > "$BLOCK_DATA_FILE" || { echo "Failed to export block"; continue; }
    
    IS_PARTIAL=$(is_likely_chunk "$SIZE")

    info_file="${OUTPUT_DIR}/protobuf_${SIZE}_${MISSING}.info.json"
    if [[ ! -f "$info_file" ]]; then
    	echo "Error: Protobuf info file not created: $info_file"
    	exit 1
    fi

    if [ "$MISSING" -eq 1 ]; then
      ipfs block rm "$CID" >/dev/null 2>&1 || true
    fi
  done
done

# Cleanup
rm "$TEMP_DIR/protobuf_generator"

echo -e "\n=== Test data generation complete. ==="
echo "Output directory: $OUTPUT_DIR"
