package br.com.oab.cdlspc.acaoagendada;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;
import com.sankhya.util.XMLUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;
import org.jdom.Document;
import org.jdom.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.stream.Collectors;

public class InclExcSpcJob implements ScheduledAction {

    private static final Logger log = LoggerFactory.getLogger(InclExcSpcJob.class);



    public void onTime(ScheduledActionContext scheduledActionContext) {

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();

        try {
            ServiceContext ctx = new ServiceContext(null);
            ctx.makeCurrent();
            BigDecimal count = NativeSql.getBigDecimal("ISNULL(COUNT(1),0)", "AD_DADOSINCEXCSPC", "PROCESSADO ='N' AND SITUACAO IS NOT NULL");

            if (BigDecimalUtil.isNullOrZero(count)) {
                log.info("[LOG SPC] ###### Nenhum dado pendente para inclusão ou exclusão.");
                return;
            }

            log.info("[LOG SPC] ###### Processando {} registros pendentes", count);
            processarPendencias();

        } catch (Exception e) {
            log.error("[LOG SPC] ###### Erro ao executar rotina SPC", e);
        }

    }

    private void processarPendencias() throws Exception {

        JapeSession.SessionHandle hnd = JapeSession.open();
        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwf.getJdbcWrapper();
        jdbc.openSession();

        try (ResultSet rs = new NativeSql(jdbc)
                .appendSql("SELECT TOP 200 SEQUENCIA, XML, SITUACAO, CODPARC, NUFIN FROM AD_DADOSINCEXCSPC WHERE PROCESSADO = 'N' ORDER BY SEQUENCIA")
                .executeQuery()) {

            while (rs.next()) {
                final BigDecimal seq = rs.getBigDecimal("SEQUENCIA");
                final String xml = rs.getString("XML");
                final String situacao = rs.getString("SITUACAO");
                final BigDecimal codparc = rs.getBigDecimal("CODPARC");
                final BigDecimal nufin = rs.getBigDecimal("NUFIN");

                hnd.execWithTX(() -> {
                    try {
                        String url = NativeSql.getString("TEXTO", "TSIPAR", "CHAVE = 'URLSPCPROD'");
                        String auth = NativeSql.getString("TEXTO", "TSIPAR", "CHAVE = 'SENHASPC'");

                        HttpClient client = new HttpClient();
                        PostMethod post = new PostMethod(url);
                        post.addRequestHeader("Authorization", "Basic " + auth);
                        post.setRequestEntity(new StringRequestEntity(xml, "text/xml", "UTF-8"));

                        int responseCode = client.executeMethod(post);

                        InputStream is = post.getResponseBodyAsStream();
                        String responseBody = new BufferedReader(new InputStreamReader(is))
                                .lines().collect(Collectors.joining("\n"));

                        Document doc = XMLUtils.buildDocumentFromString(responseBody);
                        Element root = doc.getRootElement();

                        if (XMLUtils.SimpleXPath.selectSingleNode(root, ".//Body/Fault") != null) {
                            Element fault = XMLUtils.SimpleXPath.selectSingleNode(root, ".//Body/Fault");
                            atualizarRegistroErro(jdbc, seq, fault.getChildText("faultcode"), fault.getChildText("faultstring"));
                            log.warn("[LOG SPC] ###### Erro SPC para parceiro {}: {}", codparc, fault.getChildText("faultstring"));
                        } else {
                            String sucesso = situacao.equals("E") ?
                                    XMLUtils.SimpleXPath.selectSingleNode(root, ".//excluirSpcResponse").getChildText("sucesso") :
                                    XMLUtils.SimpleXPath.selectSingleNode(root, ".//incluirSpcResponse").getChildText("sucesso");

                            atualizarRegistroSucesso(jdbc, seq, responseCode, sucesso);
                            atualizarFinanceiro(jdbc, nufin, situacao);
                            log.info("[LOG SPC] ###### Sucesso - Parceiro: {}, Ação: {}, Retorno: {}", codparc, situacao, sucesso);
                        }
                    } catch (Exception e) {
                        log.error("[LOG SPC] ###### Erro ao processar parceiro {}", codparc, e);
                        throw new RuntimeException(e);
                    }
                });
            }
        } finally {
            JdbcWrapper.closeSession(jdbc);
            JapeSession.close(hnd);
        }
    }

    private void atualizarRegistroErro(JdbcWrapper jdbc, BigDecimal seq, String codErro, String msgErro) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        String processado;

        if(codErro.equals("IE_SPC005.E10")){
            processado = "S";

        }else {
            processado = "N";
        }


        sql.appendSql("UPDATE AD_DADOSINCEXCSPC SET RETORNO = :MSG, CODERROR = :COD, PROCESSADO = :PROCESSADO,DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = :SEQ");
        sql.setNamedParameter("MSG", msgErro);
        sql.setNamedParameter("COD", codErro);
        sql.setNamedParameter("SEQ", seq);
        sql.setNamedParameter("PROCESSADO", processado);
        sql.executeUpdate();
    }

    private void atualizarRegistroSucesso(JdbcWrapper jdbc, BigDecimal seq, int responseCode, String msgSucesso) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("UPDATE AD_DADOSINCEXCSPC SET RETORNO = :MSG, CODERROR = :COD, PROCESSADO = 'S', DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = :SEQ");
        sql.setNamedParameter("MSG", msgSucesso);
        sql.setNamedParameter("COD", String.valueOf(responseCode));
        sql.setNamedParameter("SEQ", seq);
        sql.executeUpdate();
    }

    private void atualizarFinanceiro(JdbcWrapper jdbc, BigDecimal nufin, String situacao) throws Exception {
        String campo = situacao.equals("E") ? "AD_DHEXCSPC" : "AD_DHINCSPC";
        String campo2 = situacao.equals("E") ? null : "AD_INCSPC";

        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("UPDATE TGFFIN SET " + campo + " = GETDATE()" + (campo2 != null ? ", " + campo2 + " = 'S'" : "") + " WHERE NUFIN = :NUFIN");
        sql.setNamedParameter("NUFIN", nufin);
        sql.executeUpdate();
    }
}