package br.com.oab.cdlspc.evento;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

public class IncTituloTabSpc implements EventoProgramavelJava {
    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {}

    @Override
    public void beforeUpdate(PersistenceEvent arg0) throws Exception {
        System.out.println("[LOG] -> Iniciando beforeUpdate...");

        DynamicVO finVO = (DynamicVO) arg0.getVo();
        DynamicVO finOldVO = (DynamicVO) arg0.getOldVO();

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();

        BigDecimal nufin = finVO.asBigDecimal("NUFIN");
        BigDecimal nureneg  = finVO.asBigDecimal("NURENEG");
        String acao = "E";

        BigDecimal vlrbaixa = finVO.asBigDecimal("VLRBAIXA");
        BigDecimal vlrbaixaold = finOldVO.asBigDecimal("VLRBAIXA");
        Timestamp dhbaixa = finVO.asTimestamp("DHBAIXA");
        BigDecimal recdesp = finVO.asBigDecimal("RECDESP");

        System.out.println("[LOG] NUFIN: " + nufin);
        System.out.println("[LOG] NURENEG: " + nureneg);
        System.out.println("[LOG] VLRBAIXA: " + vlrbaixa);
        System.out.println("[LOG] VLRBAIXA_OLD: " + vlrbaixaold);
        System.out.println("[LOG] DHBAIXA: " + dhbaixa);
        System.out.println("[LOG] RECDESP: " + recdesp);

        BigDecimal existsspc = NativeSql.getBigDecimal("ISNULL(COUNT(1),0)", "AD_DADOSINCEXCSPC SPC",
                "NUFIN=? AND SITUACAO = 'I' AND PROCESSADO = 'S' " +
                        "AND NOT EXISTS (SELECT 1 FROM AD_DADOSINCEXCSPC WHERE NUFIN = SPC.NUFIN AND SITUACAO = 'E' AND PROCESSADO = 'S' AND SEQUENCIA > SPC.SEQUENCIA) " +
                        "AND SEQUENCIA = (SELECT MAX(SEQUENCIA) FROM AD_DADOSINCEXCSPC WHERE NUFIN = SPC.NUFIN AND SITUACAO = 'I')",
                new Object[]{nufin});

        System.out.println("[LOG] Existe SPC para baixa? " + existsspc);

        if (!vlrbaixa.equals(BigDecimal.ZERO) && BigDecimalUtil.isNullOrZero(vlrbaixaold) &&
                !BigDecimalUtil.isNullOrZero(existsspc) && dhbaixa != null) {

            System.out.println("[LOG] Condição de baixa atendida. Chamando incExcSpc...");
            incExcSpc(nufin, acao, jdbc);
        }

        if (nureneg != null && vlrbaixa != null  && dhbaixa != null) {

            if (!nureneg.equals(BigDecimal.ZERO) && !vlrbaixa.equals(BigDecimal.ZERO)) {

                System.out.println("[LOG] Condição de renegociação atendida. Buscando títulos...");

                NativeSql ns1 = new NativeSql(jdbc, getClass(), "dadosparaexclureneg.sql");
                ns1.setNamedParameter("P_NURENEG", nureneg);
                ns1.setNamedParameter("P_NUFIN", nufin);
                ResultSet rs1 = ns1.executeQuery();

                while (rs1.next()) {
                    BigDecimal nufinRenegociado = rs1.getBigDecimal("NUFIN");
                    System.out.println("[LOG] NUFIN renegociado encontrado: " + nufinRenegociado);

                    BigDecimal existsspcreneg = NativeSql.getBigDecimal("ISNULL(COUNT(1),0)", "AD_DADOSINCEXCSPC SPC",
                            "NUFIN=? AND SITUACAO = 'I' AND PROCESSADO = 'S' " +
                                    "AND NOT EXISTS (SELECT 1 FROM AD_DADOSINCEXCSPC WHERE NUFIN = SPC.NUFIN AND SITUACAO = 'E' AND PROCESSADO = 'S' AND SEQUENCIA > SPC.SEQUENCIA) " +
                                    "AND SEQUENCIA = (SELECT MAX(SEQUENCIA) FROM AD_DADOSINCEXCSPC WHERE NUFIN = SPC.NUFIN AND SITUACAO = 'I')",
                            new Object[]{nufinRenegociado});

                    System.out.println("[LOG] Existe SPC para renegociado? " + existsspcreneg);

                    if (!BigDecimalUtil.isNullOrZero(existsspcreneg)) {
                        System.out.println("[LOG] Enviando título renegociado para exclusão SPC...");
                        incExcSpc(nufinRenegociado, acao, jdbc);
                    }
                }
                rs1.close();
            }
        }

        System.out.println("[LOG] -> Finalizando beforeUpdate.");
    }

    @Override public void beforeDelete(PersistenceEvent event) throws Exception {}
    @Override public void afterInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}

    public void incExcSpc(BigDecimal nufin, String acao, JdbcWrapper jdbc) throws Exception {
        System.out.println("#### [SPC] Iniciando incExcSpc para NUFIN " + nufin + " com ação: " + acao);

        JapeWrapper spcDAO = JapeFactory.dao("AD_DADOSINCEXCSPC");
        String xml = null;

        NativeSql ns1 = new NativeSql(jdbc, getClass(), "dados.sql");
        ns1.setNamedParameter("P_NUFIN", nufin);

        ResultSet rs1 = ns1.executeQuery();

        while (rs1.next()) {
            xml = acao.equals("E") ? rs1.getString("XML_DATAEXC") : rs1.getString("XML_DATAINC");
            String email = rs1.getString("EMAIL");
            String telefone = rs1.getString("TELEFONE");
            BigDecimal codparc = rs1.getBigDecimal("CODPARC");

            System.out.println("[SPC] Dados para inserir:");
            System.out.println("  - CODPARC: " + codparc);
            System.out.println("  - EMAIL: " + email);
            System.out.println("  - TELEFONE: " + telefone);
            System.out.println("  - XML (original): " + xml);

            // Formatando XML
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

            StreamSource source = new StreamSource(new StringReader(xml));
            StringWriter writer = new StringWriter();
            transformer.transform(source, new StreamResult(writer));

            String formattedXml = writer.toString();
            System.out.println("  - XML (formatado): \n" + formattedXml);

            DynamicVO save = spcDAO.create()
                    .set("NUFIN", nufin)
                    .set("PROCESSADO", "N")
                    .set("CODPARC", codparc)
                    .set("EMAIL", email)
                    .set("TELEFONE", telefone)
                    .set("XML", formattedXml)
                    .set("SITUACAO", acao)
                    .save();

            System.out.println("[SPC] Registro salvo com sucesso para NUFIN: " + nufin);
        }

        rs1.close();
        System.out.println("#### [SPC] Finalizando incExcSpc para NUFIN: " + nufin);
    }
}
