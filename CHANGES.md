# pyParade Version history

## 0.4.0

* Support for names on all blocks (process, operations, datasets) using `nameè
* Possiblity to specify a name for the result dataset of an operation using `output_name`
* Improved status display
* Add supoort for Python 3

Breaking changes:

* The `description` parameter on processes/operations is no longer available. Please use `name` instead.
* The `context_func` paramter on operations has been rename to `context`.