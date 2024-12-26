# nextgis-data-migrator

## урлы для наполнения
- https://blacksea-monitoring.nextgis.com/api/resource/60/geojson - загрязнения
- https://blacksea-monitoring.nextgis.com/api/resource/100/geojson - птицы
- https://blacksea-monitoring.nextgis.com/api/resource/98/geojson - точки вывоза

## выполнение 
крон запускается каждые 2 часа, изменить частоту можно в `.github/workflows/migrate-data.yaml`
<br>можно запустить вне крона: Actions -> Migrate Data (слева) -> Run workflow
