Proceso de Clustering
===
Process for extract the information of months sales, separating for type "actual" and "anterior".

## Information
- Title:  `Proceso de Clustering`
- Authors:  `Erwing Forero` [more](https://github.com/ErwingForeroXpertClusteringKg/tree/develop_only_notebook "Project's link")

## Install & Dependence
- Python 3.8
- pipenv
- Fuentes de informacion:
  - BaseSocios - archivo
  - Coordenadas - archivo
  - universo indirecta - archivo
  - universo directa - archivo
  - Consultas (directa y indirecta) - carpetas

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
## Execution
- inside the file ```src/test.py```, change the next line:
  ```python
  ...
  bases = loop.run_until_complete(get_bases(sources, files_found,cached_data=False)) #False if is the first time
  ...
  ```
- inside the console execute the next command:
  ```bash
  python src\test.py
  ```
### Notebook

Seconly, if you use the file ```src\main.ipynb```, execute cell by cell in the order has been created.

- is important install **nest_asyncio**, for execute async inside notebook
  ```python
  ...
  # !pip install nest_asyncio
  import nest_asyncio
  nest_asyncio.apply()
  ...
  ```
- after executing all the previous cells, you only need to execute the final cell.

## Test

### Test Cluster

For test the cluster class base need the following:

- Requirements:

  1. inside the ```test\files``` folder must have a folder ```base_{n}``` with the next structure:
  
  - `n` is the number of group.
  
  ```
  ...
  |—— bases_n
  |    |—— base_consulta_directa_0.csv
  |    |—— base_consulta_indirecta_0.csv
  |    |—— base_consulta_indirecta_1.csv
  |    |—— base_coordenadas.csv
  |    |—— base_universo_directa.csv
  |    |—— base_universo_iddirecta.csv
  |    |—— result
       |    |—— ...
  ...
  ```
  this files are generated automatically when executes the process of [execution](#execution)

### Run 

For run test only need the next command inside console

```bash
  python -m unittest
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
