"""
Solution to exercises/exercise_02_sizing.py -- read this only after attempting it yourself.
"""

NUM_NODES = 8
CORES_PER_NODE = 16
MEMORY_GB_PER_NODE = 64

RESERVED_CORES_PER_NODE = 1
RESERVED_MEMORY_GB_PER_NODE = 1

TARGET_CORES_PER_EXECUTOR = 5


def main() -> None:
    usable_cores_per_node = CORES_PER_NODE - RESERVED_CORES_PER_NODE
    usable_memory_gb_per_node = MEMORY_GB_PER_NODE - RESERVED_MEMORY_GB_PER_NODE

    executors_per_node = usable_cores_per_node // TARGET_CORES_PER_EXECUTOR

    executor_memory_gb = usable_memory_gb_per_node / executors_per_node

    overhead_gb = max(0.384, 0.1 * executor_memory_gb)

    heap_gb = executor_memory_gb - overhead_gb

    total_executors = NUM_NODES * executors_per_node

    print(f"usable_cores_per_node = {usable_cores_per_node}")
    print(f"usable_memory_gb_per_node = {usable_memory_gb_per_node}")
    print(f"executors_per_node = {executors_per_node}")
    print(f"executor_memory_gb = {executor_memory_gb}")
    print(f"overhead_gb = {overhead_gb}")
    print(f"heap_gb = {heap_gb}")
    print(f"total_executors = {total_executors}")

    assert usable_cores_per_node == 15, f"expected 15, got {usable_cores_per_node}"
    assert usable_memory_gb_per_node == 63, f"expected 63, got {usable_memory_gb_per_node}"
    assert executors_per_node == 3, f"expected 3, got {executors_per_node}"
    assert executor_memory_gb == 21, f"expected 21, got {executor_memory_gb}"
    assert abs(overhead_gb - 2.1) < 0.01, f"expected ~2.1, got {overhead_gb}"
    assert abs(heap_gb - 18.9) < 0.01, f"expected ~18.9, got {heap_gb}"
    assert total_executors == 24, f"expected 24, got {total_executors}"

    print("\nAll checks passed!")


if __name__ == "__main__":
    main()
