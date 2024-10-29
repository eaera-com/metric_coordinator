# How to release a new version?

```
Step 0. git describe --tags --abbrev=0
Step 1. git tag new_version
Step 2. git push origin new_version
```
If you want to retrieve data from MT5, you need to install metatrader5 and mt5manager:

```
pip install metatrader5==5.0.4288
pip install mt5manager==5.0.4288
```