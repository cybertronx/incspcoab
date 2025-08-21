package br.com.oab.cdlspc.acaoagendada;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class IncSpcAutomatico implements ScheduledAction {
    @Override
    public void onTime(ScheduledActionContext scheduledActionContext) {

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();
        String acao = "I";

        System.out.println("[LOG SPC] ###### -> Iniciando a Inclusão de Dados SPC automaticamente...");

        try {
            ServiceContext ctx = new ServiceContext(null);
            ctx.makeCurrent();

            NativeSql ns1 = new NativeSql(jdbc, getClass(), "dadosparainclusao.sql");
            ResultSet rs1 = ns1.executeQuery();

            while (rs1.next()) {

                System.out.println("[LOG SPC] ###### -> Encontrado dados pra Inclusão SPC...");


                incExcSpc(rs1, acao);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void incExcSpc(ResultSet rs1, String acao) throws Exception {

        JapeWrapper spcDAO = JapeFactory.dao("AD_DADOSINCEXCSPC");
        String xml = null;
        BigDecimal nufin = rs1.getBigDecimal("NUFIN");

        if (acao.equals("E")) {
            xml = rs1.getString("XML_DATAEXC");
        } else {
            xml = rs1.getString("XML_DATAINC");

            System.out.println("[LOG SPC] ###### -> incluido parceiro " +rs1.getBigDecimal("CODPARC"));
        }

        String email = rs1.getString("EMAIL");
        String telefone = rs1.getString("TELEFONE");
        BigDecimal codparc = rs1.getBigDecimal("CODPARC");

        // Configurar o formatador
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        // Converter a string XML para fonte
        StreamSource source = new StreamSource(new StringReader(xml));
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);

        // Executar a transformação
        transformer.transform(source, result);

        // XML formatado
        String formattedXml = writer.toString();

        DynamicVO save = spcDAO.create()
                .set("NUFIN", nufin)
                .set("PROCESSADO", "N")
                .set("DHINC",Timestamp.valueOf(LocalDateTime.now()))
                .set("CODPARC", codparc)
                .set("EMAIL", email)
                .set("TELEFONE", telefone)
                .set("XML", formattedXml)
                .set("SITUACAO", acao)
                .save();
        //BigDecimal sequencia = save.asBigDecimal("SEQUENCIA");

    }

}
