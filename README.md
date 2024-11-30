# Waterfall Scraping.
Este proyecto tiene como objetivo realizar el **scraping** de la página web **Waterfall** para extraer datos relevantes que serán procesados y analizados posteriormente. A continuación, se detallan las responsabilidades, la configuración del entorno y la estructura del proyecto.

## Requisitos.
### Creación del Entorno Virtual.
Para ejecutar el proyecto correctamente, se recomienda crear un entorno virtual para gestionar las dependencias del proyecto.

```bash
python -m venv venv
source venv/bin/activate   # En Linux/macOS
.\venv\Scripts\activate    # En Windows
```
Instalar las librerias necesarias a través del archivo `requirements.txt`.

```bash
pip install -r requirements.txt
```

### Estructura de Carpetas.
El proyecto está organizado de la siguiente manera:
```python
WATERFALL/
│
├── data/                      # Carpeta para almacenar los datos extraídos.
│   └── WATERFALL.csv     # Archivo CSV con los datos extraídos y procesados.
│
├── docs/                      # Carpeta para documentación adicional.
│   └── waterfallResume.pdf    # Documentación detallada del proceso de scraping para Waterfall.
│
├── waterfallMain.py           # Script de Python que realiza el scraping de la página Waterfall.
│
├── .gitignore                 # Archivo para ignorar archivos no deseados en el repositorio.
│
└── requirements.txt           # Archivo con las dependencias necesarias.

```
