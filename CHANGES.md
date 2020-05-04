0.4.2 (unreleased)
------------------

- Improved: Exceptions now keep original traceback details
- Improved: Exceptions now include the operation name where the exception occured
- Improved: Show if an operation failed in status display
- Improved: Improved stop handling in many operations
- Improved: Adjusted test cases for Python 3
- Fixed: collect() raises exception when any operation fails instead of waiting infinitly
- Fixed: Output of empty batch in the end of batch operation
- Fixed: Potential loss of data at the end of a 
- Fixed: BTree not closing files properly

0.4.1 (2020-03-03)
------------------

- New: Batch operation
- New: Collect/ParallelProcess allow to disable status display and adjust print_status_interval
- Improved: Documentation has been extended greatly


0.4.0 (2019-11-20)
------------------