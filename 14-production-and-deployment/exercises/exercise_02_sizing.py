"""
Exercise 2 -- cluster sizing arithmetic (Lesson 2's methodology, different numbers).
No Spark needed here -- this is the reasoning a real sizing exercise requires before
you ever touch a cluster config.

Fill in every `# TODO`. Run with:

    python 14-production-and-deployment/exercises/exercise_02_sizing.py

Don't peek at solutions/solution_02_sizing.py until you've tried this.
"""

# Cluster: 8 nodes, 16 cores each, 64GB RAM each.
NUM_NODES = 8
CORES_PER_NODE = 16
MEMORY_GB_PER_NODE = 64

RESERVED_CORES_PER_NODE = 1  # for OS/daemons
RESERVED_MEMORY_GB_PER_NODE = 1

TARGET_CORES_PER_EXECUTOR = 5  # the standard sweet spot (Lesson 2)


def main() -> None:
    # TODO 1: usable cores/memory per node, after reserving for OS/daemons
    usable_cores_per_node = None  # <-- replace: CORES_PER_NODE - RESERVED_CORES_PER_NODE
    usable_memory_gb_per_node = None  # <-- replace: MEMORY_GB_PER_NODE - RESERVED_MEMORY_GB_PER_NODE

    # TODO 2: how many executors fit on one node, given TARGET_CORES_PER_EXECUTOR
    #   (integer division -- an executor needs a WHOLE number of cores)
    executors_per_node = None  # <-- replace

    # TODO 3: memory per executor (usable memory per node, split evenly across
    #   executors_per_node)
    executor_memory_gb = None  # <-- replace

    # TODO 4: off-heap overhead per executor: max(0.384, 0.1 * executor_memory_gb)
    #   (384MB expressed in GB, per Spark's default memoryOverhead formula)
    overhead_gb = None  # <-- replace

    # TODO 5: actual usable heap per executor after subtracting the overhead
    heap_gb = None  # <-- replace

    # TODO 6: total executors across the whole cluster
    total_executors = None  # <-- replace: NUM_NODES * executors_per_node

    print(f"usable_cores_per_node = {usable_cores_per_node}")
    print(f"usable_memory_gb_per_node = {usable_memory_gb_per_node}")
    print(f"executors_per_node = {executors_per_node}")
    print(f"executor_memory_gb = {executor_memory_gb}")
    print(f"overhead_gb = {overhead_gb}")
    print(f"heap_gb = {heap_gb}")
    print(f"total_executors = {total_executors}")

    # ---- self-check ----
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
