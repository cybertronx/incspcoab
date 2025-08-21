package br.com.oab.cdlspc.acao;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.StringUtils;

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

public class IncExcSpcManual implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao ctx) throws Exception {

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();
        String mensagem = null;

       String acao = ctx.getParam("P_ACAO").toString();


        for (int i = 0; i < (ctx.getLinhas()).length; i++) {

            Registro linha = ctx.getLinhas()[i];
            BigDecimal nufin = (BigDecimal) linha.getCampo("NUFIN");

            try {

                incExcSpc(nufin,acao,jdbc);

                mensagem = StringUtils.blankWhenEmpty(mensagem)+"\nTitulo Incluido na fila com Sucesso: "+ nufin;

            } catch (Exception e) {


                throw new Exception("Erro não foi possível incluir o Título  "+ nufin + " no SPC favor verifque os dados do parceiro, Telefone, Dt de Nascimento ou Endereço");

            }
        }

        ctx.setMensagemRetorno(mensagem);

    }

    public void incExcSpc(BigDecimal nufin, String acao, JdbcWrapper jdbc) throws Exception {


        System.out.println("#### Iniciando a Inclusão de Dados SPC "+ nufin.toString() + " Acao "+ acao);

        JapeWrapper spcDAO = JapeFactory.dao("AD_DADOSINCEXCSPC");
        String xml = null ;

        NativeSql ns1 = new NativeSql(jdbc, getClass(), "dadosparaincexc.sql");
        ns1.setNamedParameter("P_NUFIN", nufin);

        ResultSet rs1 = ns1.executeQuery();

        while (rs1.next()) {

            if(acao.equals("E")) {
                xml = rs1.getString("XML_DATAEXC");
            }else{
                xml = rs1.getString("XML_DATAINC");
            }
            String email = rs1.getString("EMAIL");
            String telefone = rs1.getString("TELEFONE");
            BigDecimal codparc  = rs1.getBigDecimal("CODPARC");

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


            System.out.println("#### XML "+formattedXml);

            DynamicVO save = spcDAO.create()
                    .set("NUFIN", nufin)
                    .set("PROCESSADO","N")
                    .set("DHINC", Timestamp.valueOf(LocalDateTime.now()))
                    .set("CODPARC",codparc)
                    .set("EMAIL",email)
                    .set("TELEFONE",telefone)
                    .set("XML",formattedXml)
                    .set("SITUACAO",acao)
                    .save();
        }
        rs1.close();

    }


}
