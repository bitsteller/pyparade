0.4.7 (2020-06-24)
------------------

- Improved: Improved debugging output on end of operation
- Improved: Adjusted some test cases
- Fixed: Potential deadlock at end of operation


0.4.6 (2020-06-24)
------------------

- New: Optional debug mode for verbose output


0.4.5 (2020-06-23)
------------------

- Improved: Show status 'Finishing' when operation has finished processing and is writing output
- Fixed: Minor fix when finishing operation


0.4.4 (2020-06-23)
------------------

- Fixed: Infinite wait at end of operation due to operation stopped before output has ended 
- Fixed: Operation not finishing due to batch size bigger than buffer size
- Fixed: Debug output on buffer end removed


0.4.3 (2020-05-14)
------------------

- Fixed: Calculation of buffer size which could lead to a dataset not completing


0.4.2 (2020-05-04)
------------------

- Improved: Exceptions now keep original traceback details
- Improved: Exceptions now include the operation name where the exception occured
- Improved: Show if an operation failed in status display
- Improved: Improved stop handling in many operations
- Improved: Adjusted test cases for Python 3
- Fixed: collect() raises exception when any operation fails instead of waiting infinitly
- Fixed: Output of empty batch in the end of batch operation
- Fixed: Potential loss of data at the end of a datasource
- Fixed: BTree not closing files properly

0.4.1 (2020-03-03)
------------------

- New: Batch operation
- New: Collect/ParallelProcess allow to disable status display and adjust print_status_interval
- Improved: Documentation has been extended greatly


0.4.0 (2019-11-20)
------------------