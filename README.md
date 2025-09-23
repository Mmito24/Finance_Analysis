# Finance_Analysis
Análisis financiero de acciones para el seminario de Innovacion
Para mantener actualizada la información, la plataforma integra un sistema automatizado de descarga y procesamiento de datos, alojado en GitHub. 
Este sistema está configurado para ejecutarse de lunes a viernes, posterior al cierre de operaciones bursátiles (4:00 p.m. hora local), garantizando así la incorporación de datos recientes y relevantes.
El sistema consta de una sección de programas y otra de archivos.
 
Además, el sistema hace uso de dos componentes operativos principales:
1.	Programas en Python: utilizan la API de yfinance para descargar los archivos en formato .csv correspondientes a cada una de las acciones seleccionadas. Posteriormente, consolidan la información en un solo archivo en formato .json y generan cálculos estadísticos relevantes, tales como indicadores de riesgo, relación precio-beneficio (P/E ratio), rendimientos históricos y proyecciones futuras.
2.	Workflows en GitHub: estos flujos de trabajo calendarizados automatizan la ejecución de los programas en los días y horarios establecidos, lo que asegura consistencia y confiabilidad en la actualización de datos.
 
El acceso a la información procesada se realiza directamente desde Power BI, a través de la URL correspondiente a la versión RAW de los archivos en el repositorio. En la versión web de la plataforma, los datos se actualizan automáticamente al momento de ingresar, mientras que en Power BI Desktop es necesario ejecutar la actualización manual mediante la opción Actualizar en la sección Consultas de la cinta de herramientas.
Este mecanismo no solo optimiza el proceso de recolección y análisis de datos, sino que también garantiza transparencia, trazabilidad y reproducibilidad, elementos fundamentales en proyectos de investigación aplicada y en el desarrollo de herramientas de apoyo a la toma de decisiones financieras.

