package br.com.oab.cdlspc.acaoagendada;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.BigDecimalUtil;
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

public class ExcSpcAutomatico implements ScheduledAction {
    @Override
    public void onTime(ScheduledActionContext arg0) {

            EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
            final JdbcWrapper jdbc = dwf.getJdbcWrapper();

        try {
            System.out.println("[LOG] -> Iniciando Job para Exclusão SPC...");
            gerarListaExclusao(jdbc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void gerarListaExclusao(JdbcWrapper jdbc) throws Exception{

        JapeWrapper spcDAO = JapeFactory.dao("AD_DADOSINCEXCSPC");
        String xml = null;
        BigDecimal nufin;
        NativeSql ns1 = new NativeSql(jdbc, getClass(), "dadosparaexclusao.sql");

        ResultSet rs1 = ns1.executeQuery();

        while (rs1.next()) {


            xml = rs1.getString("XML_DATAEXC");
            nufin = rs1.getBigDecimal("NUFIN");
            String email = rs1.getString("EMAIL");
            String telefone = rs1.getString("TELEFONE");
            BigDecimal codparc  = rs1.getBigDecimal("CODPARC");

            System.out.println("[LOG] -> Dados Encontrados para Exclusão...");

            System.out.println("[LOG] NUFIN: " + nufin);
            System.out.println("[LOG] CODPARC: " + codparc);


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

            LocalDateTime agora = LocalDateTime.now();
            // XML formatado
            String formattedXml = writer.toString();

            DynamicVO save = spcDAO.create()
                    .set("NUFIN", nufin)
                    .set("PROCESSADO","N")
                    .set("DHINC", Timestamp.valueOf(LocalDateTime.now()))
                    .set("CODPARC",codparc)
                    .set("EMAIL",email)
                    .set("TELEFONE",telefone)
                    .set("XML",formattedXml)
                    .set("SITUACAO","E")
                    .save();
        }
        rs1.close();

    }
}
