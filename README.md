# Proyecto de Web Scraping.
Este proyecto tiene como objetivo extraer datos relevantes de varias páginas web, limpiarlos y procesarlos para su posterior análisis. A continuación, se describe la estructura del proyecto, las herramientas utilizadas, y la configuración necesaria para ejecutarlo.

## Estructura del Proyecto.

Cada página web de la que se extraen datos está organizada en una rama específica. Cada rama contiene los siguientes elementos:

1. **Documentación**: Un archivo dónde se describe el proceso de scraping para esa página web en particular, incluyendo las URL, los tipos de datos extraídos y cualquier detalle adicional importante (como la autenticación o las cookies necesarias).

2. **Script de Scraping** (`.py`): Un archivo Python con el código necesario para realizar el scraping de la página web correspondiente.

3. **`.gitignore`**: Un archivo `.gitignore` para evitar la subida de archivos innecesarios como archivos temporales, datos sensibles o archivos de configuración locales que no deben compartirse en el repositorio.

4. **Carpeta de Datos Finales**: Una carpeta que contiene los datos extraídos después de realizar el scraping y la limpieza de los mismos. Estos datos pueden estar en formatos como **CSV**, **JSON**, o **Excel**, según sea necesario.

5. **`requirements.txt`**: Un archivo que lista todas las dependencias necesarias para ejecutar el script de scraping y el procesamiento de datos en esa rama.


## Páginas Web de Interés.
Se extraerán datos de las siguientes páginas web:
- WATERFALL:
- TISAFE:
- KONBRIEFING:
- ICSSTRIVE:
- EUROREPO:
- CYBER INCIDENTS MASTER:
- CISSM:
- HACKMAGEDDON:

## Tipos de Datos y Esquema.
Los datos a extraer incluyen:
- Nombres: Texto.
- Fechas: ISO (YYYY-MM-DD).
- Valores numéricos: Entero/Decimal.

A modo de esquema se resume en lo siguiente:

| **Campo**        | **Tipo**   | **Formato**         | **Tipo en Python** |
|------------------|------------|---------------------|--------------------|
| Nombre          | Texto      | Alfanumérico        | `str`              |
| Fecha           | Fecha      | ISO (YYYY-MM-DD)    | `str` (o `datetime.date`) |
| Valor numérico  | Número     | Entero/Decimal      | `int` o `float`     |

## Herramientas Utilizadas.

### **Scraping.**
- BeautifulSoup: Para scraping de páginas estáticas.
- Scrapy: Para proyectos a gran escala.
- Selenium: Para páginas dinámicas (JavaScript).

### **Procesamiento de Datos.**
- Pandas: Para análisis de datos.
- PySpark: Para procesamiento de Big Data.

## Configuración del Entorno.
Para ejecutar el proyecto, se recomienda crear un entorno virtual y activar el entorno adecuado según la rama del proyecto que se esté utilizando:

```bash
python -m venv venv
source venv/bin/activate   # En Linux/macOS
.\venv\Scripts\activate    # En Windows
```
Cada rama contiene un archivo `requirements.txt` con las dependencias necesarias. Instala las librerías correspondientes según la rama que estés utilizando:

```bash
pip install -r requirements.txt
```
## Integrantes.

A continuación, se detalla qué miembro del equipo ha trabajado en cada archivo **CSV** generado:

1. **CISSM.csv**:
2. **CYBER INCIDENTS MASTER.csv**: 
3. **EUROREPO.csv**: 
4. **HACKMAGEDDON.csv**: 
5. **ICSSTRIVE.csv**: 
6. **KONBRIEFING.csv**: 
7. **TISAFE.csv**: 
8. **WATERFALL.csv**: 

Cada integrante fue responsable del scraping de su respectivo archivo, documentando el proceso.

