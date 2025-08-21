package br.com.oab.cdlspc.acao;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
import com.sankhya.util.FinalWrapper;
import com.sankhya.util.StringUtils;
import com.sankhya.util.TimeUtils;
import com.sankhya.util.XMLUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.jdom.Document;
import org.jdom.Element;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class TesteInclusaoSPC implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        try {
            ServiceContext ctx = new ServiceContext(null);
            ctx.makeCurrent();
            System.out.println("#### Inicio do Teste de Envio para o SPC");
            inc_spc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void inc_spc() throws Exception {

        JapeSession.SessionHandle hdn = null;
        EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        hdn = JapeSession.getCurrentSession().getTopMostHandle();
        final JdbcWrapper jdbc = dwfEntityFacade.getJdbcWrapper();


        List<BigDecimal> listaNufins = new ArrayList<>();
        FinalWrapper<String> chaveUrlTxt = new FinalWrapper();
        FinalWrapper<String> chaveSenhaTxt = new FinalWrapper();
        NativeSql sqlBuscaUrl = getSqlBuscaParam(false, jdbc);
        sqlBuscaUrl.setReuseStatements(true);
        String chaveRepoUrlSpc = "URLSPCPROD";
        sqlBuscaUrl.setNamedParameter("CHAVE", chaveRepoUrlSpc);

        try {
            ResultSet rs = sqlBuscaUrl.executeQuery();
            if (rs.next()) chaveUrlTxt.setWrapperReference(rs.getString("TEXTO"));
            rs.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        NativeSql sqlBuscaSenha = getSqlBuscaParam(false, jdbc);
        //sqlBuscaSenha.setReuseStatements(true);
        String chaveRepoSenhaSpc = "SENHASPC";
        sqlBuscaSenha.setNamedParameter("CHAVE", chaveRepoSenhaSpc);

        try {
            ResultSet rs = sqlBuscaSenha.executeQuery();
            if (rs.next()) chaveSenhaTxt.setWrapperReference(rs.getString("TEXTO"));
            rs.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        String urlSpc = (String) chaveUrlTxt.getWrapperReference();
        String senhaSpc = (String) chaveSenhaTxt.getWrapperReference();

        System.out.println("#### Usuario " + urlSpc);
        System.out.println("#### SenhaSPC " + senhaSpc);

        System.setProperty("https.protocols", "TLSv1.2");
        System.setProperty("http.keepAlive", "false");
        final String url_conect = urlSpc;
        final String authString = senhaSpc;

        NativeSql queNotas = new NativeSql(jdbc);

        queNotas.appendSql("  SELECT SPC.SEQUENCIA SEQ, SPC.XML, SPC.SITUACAO,SPC.NUFIN,SPC.CODPARC");
        queNotas.appendSql("    FROM AD_DADOSINCEXCSPC SPC ");
        queNotas.appendSql("    INNER JOIN TGFFIN FIN ON FIN.NUFIN = SPC.NUFIN ");
        queNotas.appendSql("   WHERE SPC.PROCESSADO = 'N' ");
        queNotas.appendSql("     AND SPC.SITUACAO IS NOT NULL ");
        queNotas.appendSql("     AND FIN.DHBAIXA IS NULL ");
        queNotas.appendSql("ORDER BY 1 ");

        final ResultSet rsNomeArq = queNotas.executeQuery();

        try {
            if (rsNomeArq.next()) do {
                hdn.execWithTX(new JapeSession.TXBlock() {
                    public void doWithTx() throws Exception {
                        StringBuilder xml = new StringBuilder();
                        xml.append(rsNomeArq.getString("XML"));
                        BigDecimal nufin = rsNomeArq.getBigDecimal("NUFIN");
                        BigDecimal codparc = rsNomeArq.getBigDecimal("CODPARC");
                        HttpClient hc = new HttpClient();
                        PostMethod post = new PostMethod(url_conect);
                        post.addRequestHeader("Cache-Control", "no-cache");
                        post.addRequestHeader("Pragma", "no-cache");
                        post.addRequestHeader("SOAPAction", url_conect);
                        post.addRequestHeader("Authorization", "Basic " + authString);
                        post.setRequestEntity((RequestEntity) new StringRequestEntity(xml.toString(), "text/xml", "utf-8"));
                       // BufferedReader reader = null;
                        int result = 0;
                        try {
                            result = hc.executeMethod((HttpMethod) post);
                        } catch (Exception e) {

                            e.printStackTrace();
                            throw new Exception("Erro ao requisitar: " + e.getMessage());
                        }
                        //String responseSoap = post.getResponseBodyAsString();

                        InputStream responseStream = post.getResponseBodyAsStream();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream));
                        String line;
                        StringBuilder responseSoap = new StringBuilder();
                        while ((line = reader.readLine()) != null) {
                            responseSoap.append(line);
                        }
                        reader.close();

                        if (responseSoap != null) {
                            System.out.println("#### Resposta do SPC " + responseSoap.toString());
                        }

                        if (result != 200 && result != 500)
                            throw new Exception(String.format("Não foi possível conectar ao servidor, Código retornardo %d", new Object[]{Integer.valueOf(result)}));
                        Document docResponse = XMLUtils.buildDocumentFromString(responseSoap.toString());
                        Element element = docResponse.getRootElement();

                        try {
                            if (XMLUtils.SimpleXPath.selectSingleNode(element, ".//Body/Fault") != null) {

                                /* Erro ao Incluir ou Excluir no SPC */

                                Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element, ".//Body/Fault");
                                String codError = response_error.getChildText("faultcode");
                                String msgError = response_error.getChildText("faultstring");
                                StringBuffer queUpdateImp = new StringBuffer();

                                queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET RETORNO = '" + msgError + "', CODERROR = '" + codError + "', PROCESSADO = 'S' ,DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = " + rsNomeArq.getString("SEQ"));
                                PreparedStatement insIntercompany = jdbc.getPreparedStatement(queUpdateImp.toString());
                                insIntercompany.executeUpdate();


                            } else if (rsNomeArq.getString("SITUACAO").equals("E")) {

                                /* Verifica se a Situação é Excluir */

                                Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element, ".//excluirSpcResponse");
                                String msgError = response_error.getChildText("sucesso");
                                StringBuffer queUpdateImp = new StringBuffer();
                                StringBuffer queUpdExcImp = new StringBuffer();

                                queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET RETORNO ='" + msgError + "', CODERROR ='"+result +"', PROCESSADO = 'S' ,DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = " + rsNomeArq.getString("SEQ"));
                                PreparedStatement insIntercompany = jdbc.getPreparedStatement(queUpdateImp.toString());
                                insIntercompany.executeUpdate();

                                queUpdExcImp.append("UPDATE TGFFIN SET AD_DHEXCSPC=GETDATE() WHERE NUFIN="+nufin);
                                PreparedStatement insIntercompanyExc = jdbc.getPreparedStatement(queUpdExcImp.toString());
                                insIntercompanyExc.executeUpdate();


                            } else {

                                /* Quando a Situação é Incluir */

                                Element response_error = XMLUtils.SimpleXPath.selectSingleNode(element, ".//incluirSpcResponse");
                                String msgError = response_error.getChildText("sucesso");
                                StringBuffer queUpdateImp = new StringBuffer();
                                StringBuffer queUpdInsFin = new StringBuffer();

                                queUpdateImp.append("UPDATE AD_DADOSINCEXCSPC SET RETORNO = '" + msgError + "', CODERROR ='"+result+"' , PROCESSADO = 'S',DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = " + rsNomeArq.getString("SEQ"));
                                PreparedStatement insIntercompany = jdbc.getPreparedStatement(queUpdateImp.toString());
                                insIntercompany.executeUpdate();

                                /*Atualizar o Financeiro para marcar que o titulo esta incluso no SPC */

                                queUpdInsFin.append("UPDATE TGFFIN SET AD_DHINCSPC=GETDATE(),AD_INCSPC='S' WHERE NUFIN=" + nufin);
                                PreparedStatement insIntercompanyfin = jdbc.getPreparedStatement(queUpdInsFin.toString());
                                insIntercompanyfin.executeUpdate();

                                listaNufins.add(nufin);

                                BigDecimal qtdCriaFin = NativeSql.getBigDecimal("QTDCRIAFIN", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");

                                BigDecimal qtdinsert = NativeSql.getBigDecimal("COUNT(1)",
                                        "AD_DADOSINCEXCSPC",
                                        "PROCESSADO='S' AND SITUACAO  = 'I' AND CODERROR ='200' AND NUFINDESP IS NULL AND CODPARC = ?",codparc);


                                System.out.println("#### Teste de Update no parceiro " + codparc + "#### qtdCriaFin " + qtdCriaFin + " #### qtdinsert " + qtdinsert);

                                if (qtdCriaFin.equals(qtdinsert)) {

                                    System.out.println("#### Entrou na Inserção");

                                    BigDecimal nufindesp = gerarDespFinanceira(codparc, listaNufins);

                                    StringBuffer queUpd = new StringBuffer();
                                    queUpd.append("UPDATE AD_DADOSINCEXCSPC SET NUFINDESP = '" + nufindesp + "' WHERE CODPARC  = " + codparc + " AND NUFINDESP IS NULL AND SITUACAO = 'I' AND PROCESSADO = 'S' AND CODERROR ='200'");
                                    PreparedStatement insUp = jdbc.getPreparedStatement(queUpd.toString());
                                    insUp.executeUpdate();

                                    listaNufins.clear();


                                }


                            }
                        } catch (Exception e3) {
                            System.out.println("Erro SPC3 " + e3.getMessage());

                            e3.printStackTrace();
                            throw new RuntimeException("Erro. Não foi retornado um xml válido");
                        }
                    }
                });
            } while (rsNomeArq.next());
        } catch (Exception e) {
            System.out.println("Erro SPC " + e.getMessage());
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

    public BigDecimal gerarDespFinanceira(BigDecimal codparc, List<BigDecimal> listaNufins) throws Exception {

        BigDecimal nufindesp = BigDecimal.ZERO;
        System.out.println("#### Gerando Financeiro");

        BigDecimal codnat = NativeSql.getBigDecimal("CODNAT", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal vlrdesdob = NativeSql.getBigDecimal("VLRDESP", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codtiptit = NativeSql.getBigDecimal("CODTIPTIT", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codcencus = NativeSql.getBigDecimal("CODCENCUS", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codtipoper = NativeSql.getBigDecimal("CODTIPOPER", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");

        JapeWrapper finDAO = JapeFactory.dao("Financeiro");
        DynamicVO save = finDAO.create()
                .set("ORIGEM", "F")
                .set("CODPARC", codparc)
                .set("NUMNOTA",BigDecimal.ZERO)
                .set("CODEMP", BigDecimal.valueOf(1))
                .set("DESDOBRAMENTO", "1")
                .set("CODTIPOPER", codtipoper)
                .set("CODNAT", codnat)
                .set("CODCENCUS", codcencus)
                .set("CODTIPTIT", codtiptit)
                .set("RECDESP", BigDecimal.valueOf(1))
                .set("DTNEG", TimeUtils.getNow())
                .set("DHMOV", TimeUtils.getNow())
                .set("DTALTER", TimeUtils.getNow())
                .set("DTVENC", TimeUtils.getNow())
                .set("PROVISAO", "N")
                .set("VLRDESDOB", vlrdesdob)
                .set("HISTORICO", StringUtils.secureSubstring("Despesa Gerada Mediante a Inclusão de titulos no SPC ",1,390))
                .save();

        nufindesp = save.asBigDecimal("NUFIN");
        System.out.println("#### Financeiro Gerado");

        return  nufindesp;

    }

}
