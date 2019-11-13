# pyParade Version history

## 0.4.0

* Support for names on all blocks (process, operations, datasets)
* Improved status display
* Add supoort for Python 3

Breaking changes:

* The `description` parameter on processes/operations is no longer available. Please use `name` instead.
* The `context_func` paramter on operations has been rename to `context`.