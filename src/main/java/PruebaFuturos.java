import service.ProcesadorItems;

import java.util.Set;

public class PruebaFuturos {
    public static void main(String[] args) {
        ProcesadorItems proItems = new ProcesadorItems();
        //proItems.procesarSecuencial();
        //proItems.procesarParalelo();
        proItems.procesarPorPaquetes();
    }
}
