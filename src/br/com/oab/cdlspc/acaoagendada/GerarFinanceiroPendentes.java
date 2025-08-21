package br.com.oab.cdlspc.acaoagendada;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.TimeUtils;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class GerarFinanceiroPendentes implements ScheduledAction {

    private static final Logger log = LoggerFactory.getLogger(InclExcSpcJob.class);

    @Override
    public void onTime(ScheduledActionContext arg0) {
        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwf.getJdbcWrapper();
        NativeSql sql = null;

        log.info("[LOG SPC] ###### Iniciando o Processo de Gerar Financeiro Pendentes");

        try {
            jdbc.openSession();
            BigDecimal qtdCriaFin = NativeSql.getBigDecimal("QTDCRIAFIN", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO='S'");

            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPARC, COUNT(1) AS QTD FROM AD_DADOSINCEXCSPC " +
                    "WHERE SITUACAO = 'I' AND CODERROR IN ('200','IE_SPC001.E09') AND PROCESSADO = 'S' AND NUFINDESP IS NULL " +
                    "GROUP BY CODPARC");

            ResultSet rs = sql.executeQuery();

            while (rs.next()) {
                BigDecimal codparc = rs.getBigDecimal("CODPARC");
                int qtd = rs.getInt("QTD");
                int qtdCria = qtdCriaFin.intValue();

                int numTitulos = (int) Math.ceil((double) qtd / qtdCria);

                log.info("[LOG SPC] ###### Gerando Financeiro para o Parceiro: " +codparc.toString());
                log.info("[LOG SPC] ###### Nr de Financeiros a Serem Criados : " +numTitulos);



                for (int i = 0; i < numTitulos; i++) {
                    NativeSql sqlBusca = new NativeSql(jdbc);
                    sqlBusca.appendSql("SELECT TOP " + qtdCria + " SEQUENCIA FROM AD_DADOSINCEXCSPC " +
                            "WHERE CODPARC = :P_CODPARC AND SITUACAO = 'I' AND CODERROR IN ('200','IE_SPC001.E09') AND PROCESSADO = 'S' AND NUFINDESP IS NULL " +
                            "ORDER BY SEQUENCIA");
                    sqlBusca.setNamedParameter("P_CODPARC", codparc);

                    ResultSet rsItens = sqlBusca.executeQuery();
                    List<BigDecimal> listaSeq = new ArrayList<>();

                    while (rsItens.next()) {
                        listaSeq.add(rsItens.getBigDecimal("SEQUENCIA"));
                    }
                    rsItens.close();

                    NativeSql.releaseResources(sqlBusca);

                    if (!listaSeq.isEmpty()) {
                        BigDecimal nufin = gerarDespFinanceira(codparc, listaSeq);

                        log.info("[LOG SPC] ###### Nr Unico Financeiro Gerado : " +nufin.toString() + " - Seq: " +listaSeq.toString() + " - Parceiro: " +codparc.toString() + " - Nr de Titulos: " +qtd);

                        String inClause = listaSeq.toString().replace("[", "").replace("]", "");

                        String update = "UPDATE AD_DADOSINCEXCSPC SET NUFINDESP = " + nufin +
                                " WHERE SEQUENCIA IN (" + inClause + ")";
                        PreparedStatement ps = jdbc.getPreparedStatement(update);
                        ps.executeUpdate();
                        ps.close();
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            NativeSql.releaseResources(sql);
            JdbcWrapper.closeSession(jdbc);
        }
    }

    public BigDecimal gerarDespFinanceira(BigDecimal codparc, List<BigDecimal> listaNufins) throws Exception {
        BigDecimal codnat = NativeSql.getBigDecimal("CODNAT", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal vlrdesdob = NativeSql.getBigDecimal("VLRDESP", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codtiptit = NativeSql.getBigDecimal("CODTIPTIT", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codcencus = NativeSql.getBigDecimal("CODCENCUS", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codtipoper = NativeSql.getBigDecimal("CODTIPOPER", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codctabcoint = NativeSql.getBigDecimal("CODCTABCOINT", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");
        BigDecimal codbco = NativeSql.getBigDecimal("CODBCO", "AD_TGFSPCPARAM", "GERARDESPSPC='S' AND ATIVO ='S'");


        JapeWrapper finDAO = JapeFactory.dao("Financeiro");
        DynamicVO save = finDAO.create()
                .set("ORIGEM", "F")
                .set("CODPARC", codparc)
                .set("NUMNOTA", BigDecimal.ZERO)
                .set("CODEMP", BigDecimal.valueOf(1))
                .set("DESDOBRAMENTO", "1")
                .set("CODTIPOPER", codtipoper)
                .set("CODNAT", codnat)
                .set("CODCENCUS", codcencus)
                .set("CODTIPTIT", codtiptit)
                .set("CODBCO", codbco)
                .set("CODCTABCOINT", codctabcoint)
                .set("CODPROJ", BigDecimal.ZERO)
                .set("RECDESP", BigDecimal.valueOf(1))
                .set("DTNEG", TimeUtils.getNow())
                .set("DHMOV", TimeUtils.getNow())
                .set("DTALTER", TimeUtils.getNow())
                .set("DTVENC", TimeUtils.getNow())
                .set("PROVISAO", "N")
                .set("VLRDESDOB", vlrdesdob)
                .set("HISTORICO", "Despesa SPC para parceiro " + codparc + " - Seq: " + listaNufins)
                .save();

        return save.asBigDecimal("NUFIN");
    }
}
