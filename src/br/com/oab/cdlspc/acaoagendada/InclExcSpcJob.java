package br.com.oab.cdlspc.acaoagendada;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
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
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.stream.Collectors;

public class InclExcSpcJob implements ScheduledAction {

    private static final Logger log = LoggerFactory.getLogger(InclExcSpcJob.class);

    // Constantes para evitar "magic strings" e facilitar a manutenção
    private static final String PROCESSADO_SIM = "S";
    private static final String PROCESSADO_NAO = "N";
    private static final String SITUACAO_EXCLUSAO = "E";
    private static final String COD_ERRO_REGISTRO_INEXISTENTE = "IE_SPC005.E10";

    @Override
    public void onTime(ScheduledActionContext scheduledActionContext) {
        log.info("[LOG SPC] ###### Iniciando rotina de Inclusão/Exclusão SPC.");
        JapeSession.SessionHandle hnd = null;
        try {
            hnd = JapeSession.open();
            ServiceContext.getCurrent(); // Garante um contexto de serviço ativo

            EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
            JdbcWrapper jdbc = dwf.getJdbcWrapper();
            NativeSql countSql = new NativeSql(jdbc);
            countSql.appendSql("SELECT COUNT(1) FROM AD_DADOSINCEXCSPC WHERE PROCESSADO ='N' AND SITUACAO IS NOT NULL");

            // --- INÍCIO DA CORREÇÃO ---
            ResultSet rs = countSql.executeQuery();
            BigDecimal count = BigDecimal.ZERO; // Inicializa com zero

            // Move o cursor para a primeira (e única) linha do resultado
            if (rs.next()) {
                // Agora que estamos em uma linha válida, podemos ler o valor
                count = rs.getBigDecimal(1);
            }
            // --- FIM DA CORREÇÃO ---

            if (count == null || count.compareTo(BigDecimal.ZERO) == 0) {
                log.info("[LOG SPC] ###### Nenhum dado pendente para inclusão ou exclusão.");
                return;
            }

            log.info("[LOG SPC] ###### Processando {} registros pendentes.", count);
            processarPendencias();

        } catch (Exception e) {
            log.error("[LOG SPC] ###### Erro fatal ao executar a rotina agendada do SPC.", e);
        } finally {
            JapeSession.close(hnd);
            log.info("[LOG SPC] ###### Finalizando rotina de Inclusão/Exclusão SPC.");
        }
    }

    /**
     * Processa registros pendentes em lote, otimizando as interações com o banco de dados
     * e o tratamento de transações. Inclui validação de XML.
     */
    private void processarPendencias() throws Exception {
        JapeSession.SessionHandle hnd = JapeSession.open();
        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwf.getJdbcWrapper();
        jdbc.openSession();

        try {
            // Passo 1: Carregar configurações UMA VEZ, fora do loop.
            final String url = NativeSql.getString("TEXTO", "TSIPAR", "CHAVE = 'URLSPCPROD'");
            final String auth = NativeSql.getString("TEXTO", "TSIPAR", "CHAVE = 'SENHASPC'");

            if (url == null || auth == null) {
                log.error("[LOG SPC] ###### URL ou Autenticação do SPC não configuradas nos parâmetros 'URLSPCPROD' e 'SENHASPC'.");
                return;
            }
            final String authHeader = "Basic " + auth;

            // Inicia a transação que englobará TODO o lote.
            hnd.execWithTX(() -> {
                // Passo 2: Preparar as queries de UPDATE UMA VEZ, usando PreparedStatement para performance.
                String sqlSelect = "SELECT SEQUENCIA, XML, SITUACAO, CODPARC, NUFIN FROM AD_DADOSINCEXCSPC WHERE PROCESSADO = 'N' ORDER BY SEQUENCIA";
                String sqlUpdateSucesso = "UPDATE AD_DADOSINCEXCSPC SET RETORNO = ?, CODERROR = ?, PROCESSADO = 'S', DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = ?";
                String sqlUpdateErro = "UPDATE AD_DADOSINCEXCSPC SET RETORNO = ?, CODERROR = ?, PROCESSADO = ?, DHPROCESSAMENTO = GETDATE() WHERE SEQUENCIA = ?";
                String sqlUpdateFinInc = "UPDATE TGFFIN SET AD_DHINCSPC = GETDATE(), AD_INCSPC = 'S' WHERE NUFIN = ?";
                String sqlUpdateFinExc = "UPDATE TGFFIN SET AD_DHEXCSPC = GETDATE() WHERE NUFIN = ?";

                try (
                        ResultSet rs = new NativeSql(jdbc).appendSql(sqlSelect).executeQuery();
                        PreparedStatement psUpdateSucesso = jdbc.getConnection().prepareStatement(sqlUpdateSucesso);
                        PreparedStatement psUpdateErro = jdbc.getConnection().prepareStatement(sqlUpdateErro);
                        PreparedStatement psUpdateFinInc = jdbc.getConnection().prepareStatement(sqlUpdateFinInc);
                        PreparedStatement psUpdateFinExc = jdbc.getConnection().prepareStatement(sqlUpdateFinExc)
                ) {
                    int batchCount = 0;
                    HttpClient client = new HttpClient();

                    while (rs.next()) {
                        batchCount++;
                        final BigDecimal seq = rs.getBigDecimal("SEQUENCIA");
                        final String xml = rs.getString("XML");
                        final String situacao = rs.getString("SITUACAO");
                        final BigDecimal codparc = rs.getBigDecimal("CODPARC");
                        final BigDecimal nufin = rs.getBigDecimal("NUFIN");

                        // Passo 3: VALIDAÇÃO PRÉVIA DO XML
                        if (!isXmlWellFormed(xml, seq)) {
                            addErroToBatch(psUpdateErro, seq, "XML_INVALIDO", "O XML registrado no banco de dados é malformado.");
                            continue; // Pula para o próximo registro
                        }

                        // Passo 4: Executar a lógica de negócio para cada registro.
                        try {
                            PostMethod post = new PostMethod(url);
                            post.addRequestHeader("Authorization", authHeader);
                            post.setRequestEntity(new StringRequestEntity(xml, "text/xml", "UTF-8"));

                            int responseCode = client.executeMethod(post);

                            String responseBody;
                            try (InputStream is = post.getResponseBodyAsStream()) {
                                responseBody = new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining("\n"));
                            } finally {
                                post.releaseConnection();
                            }

                            Document doc = XMLUtils.buildDocumentFromString(responseBody);
                            Element root = doc.getRootElement();

                            if (XMLUtils.SimpleXPath.selectSingleNode(root, ".//Body/Fault") != null) {
                                Element fault = XMLUtils.SimpleXPath.selectSingleNode(root, ".//Body/Fault");
                                String faultCode = fault.getChildText("faultcode");
                                String faultString = fault.getChildText("faultstring");

                                addErroToBatch(psUpdateErro, seq, faultCode, faultString);
                                log.warn("[LOG SPC] ###### Erro retornado pelo SPC para parceiro {}: {}", codparc, faultString);
                            } else {
                                String sucesso = SITUACAO_EXCLUSAO.equals(situacao) ?
                                        XMLUtils.SimpleXPath.selectSingleNode(root, ".//excluirSpcResponse").getChildText("sucesso") :
                                        XMLUtils.SimpleXPath.selectSingleNode(root, ".//incluirSpcResponse").getChildText("sucesso");

                                addSucessoToBatch(psUpdateSucesso, seq, responseCode, sucesso);
                                addFinanceiroToBatch(psUpdateFinInc, psUpdateFinExc, nufin, situacao);
                                log.info("[LOG SPC] ###### Sucesso - Parceiro: {}, Ação: {}, Retorno: {}", codparc, situacao, sucesso);
                            }
                        } catch (Exception e) {
                            log.error("[LOG SPC] ###### Erro CRÍTICO ao processar parceiro {}. SEQ: {}.", codparc, seq, e);
                            addErroToBatch(psUpdateErro, seq, "JOB_EXCEPTION", e.getMessage());
                        }
                    }

                    // Passo 5: Executar TODOS os updates acumulados no lote DE UMA SÓ VEZ.
                    if (batchCount > 0) {
                        log.info("[LOG SPC] ###### Executando batch de updates para {} registros.", batchCount);
                        psUpdateSucesso.executeBatch();
                        psUpdateErro.executeBatch();
                        psUpdateFinInc.executeBatch();
                        psUpdateFinExc.executeBatch();
                        log.info("[LOG SPC] ###### Batch de updates finalizado.");
                    }
                }
            });

        } finally {
            JdbcWrapper.closeSession(jdbc);
            JapeSession.close(hnd);
        }
    }

    /**
     * Verifica se uma string XML é sintaticamente bem-formada.
     * @param xml A string XML a ser validada.
     * @param seq O número da sequência, para logging.
     * @return true se o XML for bem-formado, false caso contrário.
     */
    private boolean isXmlWellFormed(String xml, BigDecimal seq) {
        if (xml == null || xml.trim().isEmpty()) {
            log.warn("[LOG SPC] ###### XML vazio para a sequência {}. Marcado como inválido.", seq);
            return false;
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.parse(new InputSource(new StringReader(xml)));
            return true;
        } catch (Exception e) {
            log.error("[LOG SPC] ###### XML malformado detectado para a sequência {}. Erro: {}", seq, e.getMessage());
            return false;
        }
    }

    private void addSucessoToBatch(PreparedStatement ps, BigDecimal seq, int responseCode, String msgSucesso) throws Exception {
        ps.setString(1, msgSucesso);
        ps.setString(2, String.valueOf(responseCode));
        ps.setBigDecimal(3, seq);
        ps.addBatch();
    }

    private void addErroToBatch(PreparedStatement ps, BigDecimal seq, String codErro, String msgErro) throws Exception {
        String processado = COD_ERRO_REGISTRO_INEXISTENTE.equals(codErro) ? PROCESSADO_SIM : PROCESSADO_NAO;

        // Trunca a mensagem de erro se for muito longa para a coluna do banco
        String msgTruncada = (msgErro != null && msgErro.length() > 2000) ? msgErro.substring(0, 2000) : msgErro;

        ps.setString(1, msgTruncada);
        ps.setString(2, codErro);
        ps.setString(3, processado);
        ps.setBigDecimal(4, seq);
        ps.addBatch();
    }

    private void addFinanceiroToBatch(PreparedStatement psInc, PreparedStatement psExc, BigDecimal nufin, String situacao) throws Exception {
        if (nufin != null) { // Garante que não tente atualizar um NUFIN nulo
            if (SITUACAO_EXCLUSAO.equals(situacao)) {
                psExc.setBigDecimal(1, nufin);
                psExc.addBatch();
            } else {
                psInc.setBigDecimal(1, nufin);
                psInc.addBatch();
            }
        }
    }
}