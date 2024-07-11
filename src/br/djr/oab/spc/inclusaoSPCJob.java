
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
import com.sankhya.util.FinalWrapper;
import com.sankhya.util.XMLUtils;
import java.io.BufferedReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;
import org.jdom.Document;
import org.jdom.Element;

public class inclusaoSPCJob implements ScheduledAction {
    public void onTime(ScheduledActionContext arg0) {
        try {
            ServiceContext ctx = new ServiceContext(null);
            ctx.makeCurrent();
            inc_spc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void inc_spc() throws Exception {
        JapeSession.SessionHandle hdn = null;
        EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        String responseSoap = null;
        hdn = JapeSession.getCurrentSession().getTopMostHandle();
        final JdbcWrapper jdbc = dwfEntityFacade.getJdbcWrapper();
        FinalWrapper<String> chaveUrlTxt = new FinalWrapper();
        FinalWrapper<String> chaveSenhaTxt = new FinalWrapper();
        NativeSql sqlBuscaUrl = getSqlBuscaParam(false, jdbc);
        sqlBuscaUrl.setReuseStatements(true);
        String chaveRepoUrlSpc = "URLSPCPROD";
        sqlBuscaUrl.setNamedParameter("CHAVE", chaveRepoUrlSpc);
        try {
            ResultSet rs = sqlBuscaUrl.executeQuery();
            if (rs.next())
                chaveUrlTxt.setWrapperReference(rs.getString("TEXTO"));
            rs.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        NativeSql sqlBuscaSenha = getSqlBuscaParam(false, jdbc);
        sqlBuscaSenha.setReuseStatements(true);
        String chaveRepoSenhaSpc = "SENHASPC";
        sqlBuscaSenha.setNamedParameter("CHAVE", chaveRepoSenhaSpc);
        try {
            ResultSet rs = sqlBuscaSenha.executeQuery();
            if (rs.next())
                chaveSenhaTxt.setWrapperReference(rs.getString("TEXTO"));
            rs.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        String urlSpc = (String)chaveUrlTxt.getWrapperReference();
        String senhaSpc = (String)chaveSenhaTxt.getWrapperReference();
        System.setProperty("https.protocols", "TLSv1.2");
        System.setProperty("http.keepAlive", "false");
        final String url_conect = urlSpc;
        final String authString = senhaSpc;
        NativeSql queNotas = new NativeSql(jdbc);
        queNotas.appendSql(
                "select spc.sequencia seq, spc.xml, spc.situacao " +
                   " from ad_dadosincexcspc spc " +
                  " where spc.processado ='N' " +
               " order by 1");
        final ResultSet rsNomeArq = queNotas.executeQuery();
        try {
            if (rsNomeArq.next())
                do {
                    hdn.execWithTX(new JapeSession.TXBlock() {
                        public void doWithTx() throws Exception {
                            StringBuilder xml = new StringBuilder();
                            xml.append(rsNomeArq.getString("XML"));
                            HttpClient hc = new HttpClient();
                            PostMethod post = new PostMethod(url_conect);
                            post.addRequestHeader("Cache-Control", "no-cache");
                            post.addRequestHeader("Pragma", "no-cache");
                            post.addRequestHeader("SOAPAction", url_conect);
                            post.addRequestHeader("Authorization", "Basic " + authString);
                            post.setRequestEntity(
                                    (RequestEntity)new StringRequestEntity(xml.toString(), "text/xml", "utf-8"));
                            BufferedReader reader = null;
                            int result = 0;
                            try {
                                result = hc.executeMethod((HttpMethod)post);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new Exception("Erro ao requisitar: " + e.getMessage());
                            }
                            String responseSoap = post.getResponseBodyAsString();
                            if (result != 200 && result != 500)
                                throw new Exception(String.format(
                                        "Nfoi possconectar ao serviCretornardo %d", new Object[] { Integer.valueOf(result) }));
                            Document docResponse =
                                    XMLUtils.buildDocumentFromString(post.getResponseBodyAsString());
                            Element element = docResponse.getRootElement();
                            try {
                                if (XMLUtils.SimpleXPath.selectSingleNode(element, ".//Body/Fault") != null) {
                                    Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element,
                                            ".//Body/Fault");
                                    String codError = response_error.getChildText("faultcode");
                                    String msgError = response_error.getChildText("faultstring");
                                    StringBuffer queUpdateImp = new StringBuffer();
                                    queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET retorno = '" + msgError +
                                            "', coderror = '" + codError + "', PROCESSADO = 'S' WHERE sequencia = " +
                                            rsNomeArq.getString("SEQ"));
                                    PreparedStatement insIntercompany = jdbc
                                            .getPreparedStatement(queUpdateImp.toString());
                                    insIntercompany.executeUpdate();
                                } else if (rsNomeArq.getString("SITUACAO").equals("Excluir")) {
                                    Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element,
                                            ".//excluirSpcResponse");
                                    String msgError = response_error.getChildText("sucesso");
                                    StringBuffer queUpdateImp = new StringBuffer();
                                    queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET retorno = '" + msgError +
                                            "', coderror = '" + result + "', PROCESSADO = 'S' WHERE sequencia = " +
                                            rsNomeArq.getString("SEQ"));
                                    PreparedStatement insIntercompany = jdbc
                                            .getPreparedStatement(queUpdateImp.toString());
                                    insIntercompany.executeUpdate();
                                } else {
                                    Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element,
                                            ".//incluirSpcResponse");
                                    String msgError = response_error.getChildText("sucesso");
                                    StringBuffer queUpdateImp = new StringBuffer();
                                    queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET retorno = '" + msgError +
                                            "', coderror = '" + result + "', PROCESSADO = 'S' WHERE SEQUENCIA = " +
                                            rsNomeArq.getString("SEQ"));
                                    PreparedStatement insIntercompany = jdbc
                                            .getPreparedStatement(queUpdateImp.toString());
                                    insIntercompany.executeUpdate();
                                }
                            } catch (Exception e3) {
                                e3.printStackTrace();
                                throw new RuntimeException("Erro. Não foi retornado um xml válido");
                            }
                        }
                    });
                } while (rsNomeArq.next());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcWrapper.closeSession(jdbc);
        }
    }

    private NativeSql getSqlBuscaParam(boolean ehTexto, JdbcWrapper jdbcWrapper) {
        NativeSql sqlBuscaParam = new NativeSql(jdbcWrapper);
        sqlBuscaParam.appendSql("\tSELECT ");
        if (ehTexto) {
            sqlBuscaParam.appendSql("\t\tINTEIRO ");
        } else {
            sqlBuscaParam.appendSql("\t\tTEXTO ");
        }
        sqlBuscaParam.appendSql("\tFROM ");
        sqlBuscaParam.appendSql("\t\tTSIPAR PAR ");
        sqlBuscaParam.appendSql("\tWHERE ");
        sqlBuscaParam.appendSql("\t \tPAR.CHAVE = :CHAVE");
        return sqlBuscaParam;
    }
}
