package service;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;

public class ProcesadorItems {
    private static Logger logger = LogManager.getLogger(ProcesadorItems.class);
    private static final int[] ERRORES = {3, 13, 23, 33};

    public ProcesadorItems() {
    }

    public void procesarSecuencial() {
        Instant ini = Instant.now();
        UUID uuid = UUID.randomUUID();
        int contErrores = 0;
        List<ObjNegocio> lista = generarListadoDummy();
        logger.debug("Procesando {} items para {}", lista.size(), uuid);
        for (ObjNegocio o : lista) {
            ObjNegocio oRes = procesarItem(o, uuid);
            if (oRes.estado == 0) {
                contErrores++;
            }
        }
        logger.debug("Fin Proceso {} items para {}, errores:{}, tiempo:{}", lista.size(), uuid, contErrores, (Duration.between(ini, Instant.now()).getSeconds()));
    }

    public void procesarParalelo() {
        Instant ini = Instant.now();
        UUID uuid = UUID.randomUUID();
        int contErrores = 0;
        List<ObjNegocio> lista = generarListadoDummy();

        logger.debug("Procesando {} items para {}", lista.size(), uuid);
        List<CompletableFuture<ObjNegocio>> result =
                lista.stream()
                        .sorted(Comparator.comparingInt(ObjNegocio::getId).thenComparing(ObjNegocio::getPrioridad))
                        .map(o -> CompletableFuture.supplyAsync(
                                () -> procesarItem(o, uuid))
                        )
                        .collect(Collectors.toList());

        List<ObjNegocio> resultadosFinales = result
                .stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        contErrores = (int) resultadosFinales.stream().filter(r -> r.estado == 0).count();
        logger.debug("Fin Proceso {} items para {}, errores:{}, tiempo:{}", lista.size(), uuid, contErrores, (Duration.between(ini, Instant.now()).getSeconds()));
    }

    public void procesarPorPaquetes() {
        Instant ini = Instant.now();
        UUID uuid = UUID.randomUUID();
        int contErrores = 0;
        List<ObjNegocio> lista = generarListadoDummy();

        Map<Integer, List<ObjNegocio>> paquetes = lista.stream()
                .collect(groupingBy(ObjNegocio::getId));

        logger.debug("Procesando {} paquetes para {}", paquetes.size(), uuid);
        List<CompletableFuture<List<ProcesadorItems.ObjNegocio>>> result = paquetes.entrySet().stream()
                .map(o -> CompletableFuture.supplyAsync(
                        () -> procesarPaqueteSecuencial(o.getKey(), o.getValue(), uuid)
                        )
                ).collect(Collectors.toList());

        List<ObjNegocio> resultadosFinales = result
                .stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .flatMap(l -> l.stream())
                .collect(Collectors.toList());

        contErrores = (int) resultadosFinales.stream().filter(r -> r.estado == 0).count();
        logger.debug("Fin Proceso {} items para {}, errores:{}, tiempo:{}", lista.size(), uuid, contErrores, (Duration.between(ini, Instant.now()).getSeconds()));
    }

    private List<ObjNegocio> procesarPaqueteSecuencial(Integer id, List<ObjNegocio> paquete, UUID uuid) {
        logger.debug("Procesando paquete {}: {} items", id, paquete.size());
        for (ObjNegocio o : paquete) {
            ObjNegocio oRes = procesarItem(o, uuid);
            if (oRes.estado == 0) {
                logger.debug("Se encontro parquete con errores id {} se detiene proceso paquete",id);
                return paquete;
            }
        }
        return paquete;
    }


    private ObjNegocio procesarItem(ObjNegocio obj, UUID uuid) {
        try {
            if (IntStream.of(ERRORES).anyMatch(n -> n == obj.id)) {
                obj.estado = 0;
            }
            //Thread.sleep((long) (Math.random() * 10000 + 1));
            Thread.sleep(1000);
            logger.debug("item procesado uuid:{} id:{} prioridad:{} estado:{}", uuid, obj.id, obj.prioridad, obj.estado);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return obj;
    }

    private List<ObjNegocio> generarListadoDummy() {
        List<ObjNegocio> lista = new ArrayList<>();
        //for (int x = 0; x < (Math.random() * 10000 + 1); x++) {
        for (int x = 0; x < 20; x++) {
            lista.add(new ObjNegocio(x + 1, 1, 1));
            lista.add(new ObjNegocio(x + 1, 1, 2));
            lista.add(new ObjNegocio(x + 1, 1, 3));
        }
        return lista;
    }

    private class ObjNegocio {
        int id;
        int estado;
        int prioridad;

        public ObjNegocio(int id, int estado, int prioridad) {
            this.id = id;
            this.estado = estado;
            this.prioridad = prioridad;
        }

        public int getPrioridad() {
            return prioridad;
        }

        public int getId() {
            return id;
        }
    }
}
