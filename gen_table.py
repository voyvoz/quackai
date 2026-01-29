import pyarrow as pa
import pyarrow.ipc as ipc

def main(out_path: str = "animals.arrow"):
    data = {
        "id":   pa.array([0, 1, 2, 3, 4, 5], type=pa.int32()),
        "text": pa.array(["cat", "dog", "cat", "fish", "cat", "dog"], type=pa.string()),
    }

    table = pa.Table.from_pydict(data)

    with ipc.new_file(out_path, table.schema) as writer:
        writer.write_table(table)

    print(f"Wrote {out_path}")
    print("Schema:")
    print(table.schema)

if __name__ == "__main__":
    import sys
    out = sys.argv[1] if len(sys.argv) > 1 else "animals.arrow"
    main(out)
