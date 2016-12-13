#Spark Job for parsing GIS Data

Die Hauptlogik ist den Klassen SparkProduction und OSMParser. 

Die Runtime ist [Apache Spark](http://spark.apache.org/docs/latest/index.html) hauptsächlich läuft es jedoch als Single threaded Parser, weil Spark keinen größereren Performance Vorteil bringt (eher sogar Nachteile auf Grund des Perforamance Auswirkungen).

Falls wir jemals mehrere Datein parallel verarbeiten wollen (>20) macht es Sinn das auf Spark anzupassen. Essentiell muss einfach die Parsing Logik in [`map`](http://spark.apache.org/docs/latest/programming-guide.html#rdd-operations)  Funktionen gepackt werden.

##Konfiguration 

Die Konfiguration des Jobs ist augelagert in [application.conf](src/main/resources/application.conf). Diese Datei enthält alles wesentlichen Optionen. 

### Building 
Apache Maven basiertes Projekt. Gebaut wird mit folgendem Befehl: 
```bash
mvn clean compile package
```
Resultierendes JAR File aus dem `target` folder kann deployed werden. 

## Todos 
Wir müssen prüfen wie es alles auf dem Cluster funktioniert. Insbesondere müssen wir inwweit wir relevante Dateien wie `hdfs-site.xml` und `core-site.xml` noch in das JAR hinzufügen müssen.
Weiterhin ist der Unit Test eher rudimentär (bis jetzt).

## Verwendete Tools
+ [Apache Spark](http://spark.apache.org/)
+ [Geotools](http://docs.geotools.org/)
+ [Hadoop API](https://hadoop.apache.org/docs/stable2/api/index.html)