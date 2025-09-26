#!/bin/bash

# Function to get available memory in KB
get_available_memory() {
    available_mem=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    echo $available_mem
}

# Function to convert KB to bytes
kb_to_bytes() {
    echo $(($1 * 1024))
}

# Step 1: Calculate memory sizes
#available_mem_kb=$(get_available_memory)
#available_mem_bytes=$(kb_to_bytes $available_mem_kb)
#memory_to_allocate_kb=$((available_mem_kb * 80 / 100))
#remaining_mem_kb=$((available_mem_kb - memory_to_allocate_kb))
#file_size_kb=$((remaining_mem_kb * 2))

available_mem=$(get_available_memory)
percent_mem=$((available_mem * 99 / 100))
eighth_gb=$((1024 * 1024 /8))
remaining_mem=$((available_mem - memory_to_allocate))
memory_to_allocate=$percent_mem
if [ $remaining_mem -gt $eighth_gb ]; then
  memory_to_allocate=$((available_mem - eighth_gb))
fi

echo "Available memory: $available_mem_kb KB"
echo "Memory to allocate : $memory_to_allocate KB"
echo "Remaining memory : $remaining_mem KB"

# Step 1 & 2: Allocate memory using stress-ng and keep it allocated
echo "Allocating 80% of available memory..."
stress_pid=""
stress-ng --vm 1 --vm-bytes ${memory_to_allocate}K --vm-hang 5 &
stress_pid=$!
sleep 5  # Give stress-ng time to allocate memory

# Step 3: Create a large file (twice the size of remaining memory)
echo "Creating large file (twice the size of remaining memory)..."
large_file="/home/ec2-user/scripts/large_file_for_cache_clear"
dd if=/dev/urandom of=$large_file bs=1M count=1024 status=progress

# Step 4: Sync to ensure file is written to disk
echo "Syncing file system..."
sudo sync
num_parallel=6
pids=()
for i in $(seq 1 $num_parallel); do
    echo "dd if=$large_file of=/dev/null bs=1M status=none"	
    dd if=$large_file of=/dev/null bs=1M status=none &
    pids+=($!)
done

# Wait for all reads to complete
for pid in ${pids[@]}; do
    wait $pid
done
echo "All reads completed"


echo "Stopping the stress ng process"
if [ ! -z "$stress_pid" ]; then
  kill $stress_pid
  wait $stress_pid 2>/dev/null || true
fi

# Step 5 & 6: Read the file in parallel with different block sizes
echo "Reading file in parallel to fill page cache..."
# Calculate number of parallel reads and block sizes


# Start parallel reads


# Step 7: Free the allocated memory


# Step 8: Sync again
echo "Syncing file system again..."
sudo sync

# Step 9: Drop caches
echo "Dropping caches..."
sudo sh -c "echo 1 > /proc/sys/vm/drop_caches"

# Clean up
echo "Cleaning up..."
rm -f $large_file
echo "Page cache clearing completed successfully"
