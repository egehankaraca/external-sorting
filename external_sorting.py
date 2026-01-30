#Egehan Karaca 2682896
import os
import heapq

#Task1
def get_product_id(record):
    id_bytes = record[:4]
    return int.from_bytes(id_bytes, byteorder='little', signed=False)

def merge_sort(records, ascending):
    if len(records) <= 1:
        return records
    else:
        mid = len(records) // 2
        left = merge_sort(records[:mid], ascending)
        right = merge_sort(records[mid:], ascending)
        return merge(left, right, ascending)

def merge(list1, list2, ascending):
    sorted_list = []
    i = 0
    j = 0
    len_left = len(list1)
    len_right = len(list2)

    while i < len_left and j < len_right:
        left_id = int.from_bytes(list1[i][:4], 'little')
        right_id = int.from_bytes(list2[j][:4], 'little')
        pick_left = False
        if ascending:
            if left_id <= right_id:
                pick_left = True
        else:
            if left_id >= right_id:
                pick_left = True
        if pick_left:
            sorted_list.append(list1[i])
            i += 1
        else:
            sorted_list.append(list2[j])
            j += 1

    sorted_list.extend(list1[i:])
    sorted_list.extend(list2[j:])

    return sorted_list

def generate_runs(input_filename, buffer_pages,
                  page_size, record_size, output_dir,
                  ascending = True, unique = False):
    target_file_size = buffer_pages * page_size
    records_per_run = target_file_size // record_size
    bytes_to_read = records_per_run * record_size
    run_counter = 0
    run_files = []

    with open(input_filename, "rb") as input_file:
        while True:
            chunk = input_file.read(bytes_to_read)
            if not chunk:
                break
            records = []
            for i in range(0, len(chunk), record_size):
                rec = chunk[i:i + record_size]
                if len(rec) == record_size:
                    records.append(rec)
            if not records:
                break

            sorted_records = merge_sort(records, ascending)
            unique_records = []

            if unique:
                unique_records.append(sorted_records[0])
                prev = 0
                curr = 1

                while curr < len(sorted_records):

                    if sorted_records[curr] != sorted_records[prev]:
                        unique_records.append(sorted_records[curr])
                        prev = curr

                    curr += 1
            else:
                unique_records = sorted_records

            if not unique_records:
                continue

            #padding
            data_bytes = b"".join(unique_records)
            padding_needed = target_file_size - len(data_bytes)
            final_bytes = data_bytes

            if padding_needed > 0:
                final_bytes += b'\x00' * padding_needed

            filename = os.path.join(output_dir, f"run_{run_counter}.bin")
            run_files.append(filename)
            with open(filename, "wb") as output_file:
                output_file.write(final_bytes)

            run_counter += 1

    return run_files

#Task2
def merge_runs(run_filenames, output_filename, buffer_pages,
               page_size, record_size, ascending = True,
               unique = False):
    open_files = []
    for filename in run_filenames:
        try:
            open_files.append(open(filename, "rb"))
        except FileNotFoundError:
            print(f"File {filename} not found")

    min_heap = []
    null_record = b'\x00' * record_size

    for file_idx, f in enumerate(open_files):
        rec = f.read(record_size)

        if rec and (rec == null_record or len(rec) != record_size):
            rec = None
        if rec:
            product_id = get_product_id(rec)

            if ascending:
                sort_key = product_id
            else:
                sort_key = -product_id
            heapq.heappush(min_heap, (sort_key, file_idx, rec))

    output_buffer = []
    max_buffer = buffer_pages * page_size
    curr_bytes = 0
    last_written_record = None


    with open(output_filename, "wb") as output_file:
        while min_heap:
            key, file_idx, record = heapq.heappop(min_heap)

            should_write = True
            if unique:
                if last_written_record == record:
                    should_write = False
                else:
                    should_write = True
                    last_written_record = record

            if should_write:
                output_buffer.append(record)
                curr_bytes += record_size

            if curr_bytes >= max_buffer:
                output_file.write(b"".join(output_buffer))
                output_buffer = []
                curr_bytes = 0

            next_record = open_files[file_idx].read(record_size)
            if next_record and (next_record == null_record or len(next_record) != record_size):
                next_record = None
            if next_record:
                next_id = get_product_id(next_record)
                if ascending:
                    sort_key = next_id
                else:
                    sort_key = -next_id
                heapq.heappush(min_heap, (sort_key, file_idx, next_record))

        if output_buffer:
            output_file.write(b"".join(output_buffer))

        current_size = output_file.tell()
        chunk_size = buffer_pages * page_size

        remainder = current_size % chunk_size
        if remainder > 0:
            padding_needed = chunk_size - remainder
            output_file.write(b'\x00' * padding_needed)

        for f in open_files:
            f.close()
    return

#Task3

def external_sort(input_filename, output_filename, buffer_pages,
                  page_size, record_size, ascending = True,
                  unique = False):

    output_dir = os.path.dirname(output_filename)
    files = generate_runs(input_filename, buffer_pages,
                  page_size, record_size, output_dir,
                  ascending, unique)

    num_runs = len(files)
    num_passes = 1

    while len(files) > 1:
        next_files = []
        k = buffer_pages - 1
        run_index = 0
        for i in range (0, len(files), k):
            group = files[i:i + k]
            filename = f"out_pass_{num_passes}_run_{run_index}.bin"
            path = os.path.join(output_dir, filename)

            merge_runs(group, path, buffer_pages, page_size, record_size, ascending, unique)
            next_files.append(path)
            run_index += 1

        files = next_files
        num_passes += 1

    final_file = files[0]
    null_record = b'\x00' * record_size
    with open(final_file, "rb") as src:
        with open(output_filename, "wb") as dst:
            bytes_written = 0

            while True:
                rec = src.read(record_size)
                if not rec or len(rec) != record_size:
                    break

                if rec == null_record:
                    break
                dst.write(rec)

    return {
        "num_runs": num_runs,
        "num_passes": num_passes,
        "output_file": output_filename
    }



