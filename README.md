Proceso de Clustering
===
Process for extract the information of months sales, separating for type "actual" and "anterior".

## Information
- Title:  `Proceso de Clustering`
- Authors:  `Erwing Forero` [more](https://github.com/ErwingForeroXpertClusteringKg/tree/develop_only_notebook "Project's link")

## Install & Dependence
- Python 3.8
- pipenv

## Use
- Install dependencies

  ```bash
  pipenv install
  ```
- Verify the file ```config.yml```

  ```yaml
  sources: contains the structure of xls* files
  order_base: contains the order in which it will be read
  final_base: contains the result columns and their order in the final base
  ```
- - Structure of **sources**:
  ```yaml
  sources:
    ... 
    name: name of base [str]
        type: type of base, "folder" or "file" [str]
        sheet: name of sheet [str]
        skiprows: skip rows? [int]
        encoding: codification of file or files, [str]
        columns: 
            - column: name of column [str]
              pos: position of values, ej: 1 or '1-3' [str|int]
        converters:
            name of column [str]: "text" or "number" [str]
    ...
  ```
- copy and paste the xls* files in the following path ```files\Bases```

  ```
  |—— files
  |    ...
  |    |—— Bases
  |        |—— BaseSocios.xlsm
  |        |—— Directa
  |        |—— DriverCoordenadas.xlsx
  |        |—— Indirecta
  |        |—— Universo Directa.xlsm
  |        |—— Universo Indirecta.xlsm
      ...
  ```

## Directory Hierarchy
```
|—— .gitignore
|—— config.yml
|—— files
|    |—— alerts
|    |—— Bases
|        |—— BaseSocios.xlsm
|        |—— Directa
|        |—— DriverCoordenadas.xlsx
|        |—— Indirecta
|        |—— Universo Directa.xlsm
|        |—— Universo Indirecta.xlsm
|    |—— temp
|    |—— utils
|—— Pipfile
|—— Pipfile.lock
|—— proceso.txt
|—— repositorio.txt
|—— requirements.txt
|—— src
|    |—— cluster
|        |—— cluster.py
|        |—— cluster_types.py
|        |—— __init__.py
|    |—— dataframes
|        |—— dataframe_optimized.py
|        |—— func.py
|        |—— __init__.py
|    |—— gui
|    |—— main.ipynb
|    |—— test.py
|    |—— utils
|        |—— constants.py
|        |—— feature_flags.py
|        |—— index.py
|        |—— log.py
|        |—— __init__.py

```
## Code Details
### Tested Platform

- software
  ```
  OS: Windows 10 Home Edition
  Python: 3.8.5 
  ```
- hardware
  ```
  CPU: Ryzen 5
  GPU: None
  ```
  
## License

MIT
