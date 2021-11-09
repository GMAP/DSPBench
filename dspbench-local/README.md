# TODO

 - Renomear de local para java threads
 - Limitar buffer do LocalOperatorInstance
 - Documentar como foi implementado (filas, distribuição, relação produtor/consumidor, tipo de fila)


 - deixar modular o projeto (apartar módulos)
 - como validar os resultados?
 - adicionar determinismo nos datasets - operadores (não usar geradores de dados - usar datasets fixos)
 - comparar resultado salvo em arquivo com o resultado esperado usando hash do conteúdo
 - comitar na branch v2


```
../gradlew clean build shadowJar -x test -Dorg.gradle.java.home=/home/mayconbordin/.sdkman/candidates/java/11.0.11-open 
```

```
bin/dspbench-local.sh build/libs/dspbench-local-1.0-all.jar com.streamer.examples.wordcount.WordCountTask WordCount src/main/resources/config/word-count.properties 
```


```
docker build -t dspbench-local .
```

```
docker run -it dspbench-local com.streamer.examples.wordcount.WordCountTask WordCount /app/config/word-count.properties
```