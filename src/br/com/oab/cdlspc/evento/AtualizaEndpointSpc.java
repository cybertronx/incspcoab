package br.com.oab.cdlspc.evento;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.internal.CoreParameterImpl;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.sql.PreparedStatement;
import java.util.Base64;

public class AtualizaEndpointSpc implements EventoProgramavelJava {
    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void afterInsert(PersistenceEvent arg0) throws Exception {

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();

        DynamicVO spcVO = (DynamicVO) arg0.getVo();

        String situacao = spcVO.asString("ATIVO");
        String usuario = spcVO.asString("USUARIO");
        String senha = spcVO.asString("SENHA");
        String urlProd = spcVO.asString("URLPROD");
        String urlTest = spcVO.asString("URLTESTE");
        String tipo = spcVO.asString("TIPO");
        String pw = usuario + ":" + senha;
        String pwb64 = encodeToBase64(pw);
        String urlB = null;


        if (situacao.equals("S") && tipo.equals("T")) {

            urlB = urlTest;

        } else if (situacao.equals("S") && tipo.equals("P")) {

            urlB = urlProd;

        }

        setarUrlSenha(urlB, pwb64);


    }

    @Override
    public void afterUpdate(PersistenceEvent arg0) throws Exception {

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();

        DynamicVO spcVO = (DynamicVO) arg0.getVo();

        String situacao = spcVO.asString("ATIVO");
        String usuario = spcVO.asString("USUARIO");
        String senha = spcVO.asString("SENHA");
        String urlProd = spcVO.asString("URLPROD");
        String urlTest = spcVO.asString("URLTESTE");
        String tipo = spcVO.asString("TIPO");
        String urlB = null;

        String pw = usuario + ":" + senha;

        String pwb64 = encodeToBase64(pw);


        if (situacao.equals("S") && tipo.equals("T")) {

            urlB = urlTest;

        } else if (situacao.equals("S") && tipo.equals("P")) {

            urlB = urlProd;

        }

        setarUrlSenha(urlB, pwb64);


    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeCommit(TransactionContext tranCtx) throws Exception {

    }


    public static String encodeToBase64(String text) {

        return Base64.getEncoder().encodeToString(text.getBytes());

    }

    public void setarUrlSenha(String url, String senha) throws Exception {


        CoreParameterImpl coreParameterUrl = new CoreParameterImpl();
        CoreParameterImpl coreParameterSenha = new CoreParameterImpl();
        coreParameterUrl.saveParameter("URLSPCPROD", "TEXTO", url, "T", "Url Integração SPC OAB");
        coreParameterSenha.saveParameter("SENHASPC", "TEXTO", senha, "T", "Senha padrão do SPC OAB");


    }

}
