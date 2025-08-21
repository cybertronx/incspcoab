SELECT
    CONCAT(
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="http://webservice.spc.insumo.spcjava.spcbrasil.org/">',
        '<soapenv:Header/>',
        '<soapenv:Body>',
        '<web:incluirSpc>',
        '<insumoSpc>',
        CASE WHEN PAR.TIPPESSOA IS NOT NULL THEN '<tipo-pessoa>' + PAR.TIPPESSOA + '</tipo-pessoa>' ELSE '' END,
        '<dados-pessoa-fisica>',
        CASE WHEN PAR.CGC_CPF IS NOT NULL THEN '<cpf numero="' + TRIM(PAR.CGC_CPF) + '"/>' ELSE '' END,
        CASE WHEN PAR.NOMEPARC IS NOT NULL OR PAR.RAZAOSOCIAL IS NOT NULL THEN '<nome>' + UPPER(TRIM(ISNULL(PAR.NOMEPARC, PAR.RAZAOSOCIAL))) + '</nome>' ELSE '' END,
        CASE WHEN PAR.DTNASC IS NOT NULL THEN '<data-nascimento>' + FORMAT(PAR.DTNASC, 'yyyy-MM-ddTHH:mm:ss') + '</data-nascimento>' ELSE '' END,
        CASE WHEN PAR.EMAIL IS NOT NULL THEN '<email>' + TRIM(UPPER(PAR.EMAIL)) + '</email>' ELSE '' END,
        CASE WHEN PAR.TELEFONE IS NOT NULL THEN '<telefone numero-ddd="' + TRIM(SUBSTRING(PAR.TELEFONE, 0, 3)) + '" numero="' + TRIM(SUBSTRING(PAR.TELEFONE, 3, 9)) + '"/>' ELSE '' END,
        '</dados-pessoa-fisica>',
           CASE
            WHEN FIN.DTNEG IS NOT NULL AND FIN.DTVENC IS NOT NULL THEN
                '<data-compra>' + FORMAT(
                    CASE
                        WHEN FIN.DTNEG > FIN.DTVENC THEN FIN.DTVENC
                        ELSE FIN.DTNEG
                    END,
                'yyyy-MM-ddTHH:mm:ss') + '</data-compra>'
            ELSE ''
        END,
        CASE WHEN FIN.DTVENC IS NOT NULL THEN '<data-vencimento>' + FORMAT(FIN.DTVENC, 'yyyy-MM-ddTHH:mm:ss') + '</data-vencimento>' ELSE '' END,
        '<codigo-tipo-devedor>C</codigo-tipo-devedor>',
        CASE WHEN FIN.NUFIN IS NOT NULL THEN '<numero-contrato>' + CAST(FIN.NUFIN AS VARCHAR) + '</numero-contrato>' ELSE '' END,
        CASE WHEN FIN.VLRDESDOB IS NOT NULL THEN '<valor-debito>' + TRIM(REPLACE(CAST(FIN.VLRDESDOB AS DECIMAL(7, 2)), ',', '.')) + '</valor-debito>' ELSE '' END,
        '<natureza-inclusao>',
        '<id>75</id>',
        '</natureza-inclusao>',
        '<endereco-pessoa>',
        CASE WHEN PAR.CEP IS NOT NULL THEN '<cep>' + TRIM(PAR.CEP) + '</cep>' ELSE '' END,
        CASE WHEN ED.NOMEEND IS NOT NULL THEN '<logradouro>' + TRIM(UPPER(ED.NOMEEND)) + '</logradouro>' ELSE '' END,
        CASE WHEN BAI.NOMEBAI IS NOT NULL THEN '<bairro>' + UPPER(TRIM(BAI.NOMEBAI)) + '</bairro>' ELSE '' END,
        CASE WHEN PAR.NUMEND IS NOT NULL THEN '<numero>' + TRIM(PAR.NUMEND) + '</numero>' ELSE '' END,
        '</endereco-pessoa>',
        '</insumoSpc>',
        '</web:incluirSpc>',
        '</soapenv:Body>',
        '</soapenv:Envelope>'
    ) AS XML_DATAINC,
    CONCAT(
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="http://webservice.spc.insumo.spcjava.spcbrasil.org/">',
        '<soapenv:Header/>',
        '<soapenv:Body>',
        '<web:excluirSpc>',
        '<excluir>',
        CASE WHEN PAR.TIPPESSOA IS NOT NULL THEN '<tipo-pessoa>' + PAR.TIPPESSOA + '</tipo-pessoa>' ELSE '' END,
        '<dados-pessoa-fisica>',
        CASE WHEN PAR.CGC_CPF IS NOT NULL THEN '<cpf numero="' + TRIM(PAR.CGC_CPF) + '"/>' ELSE '' END,
        '</dados-pessoa-fisica>',
        CASE WHEN FIN.DTVENC IS NOT NULL THEN '<data-vencimento>' + FORMAT(FIN.DTVENC, 'yyyy-MM-ddTHH:mm:ss') + '</data-vencimento>' ELSE '' END,
        CASE WHEN FIN.NUFIN IS NOT NULL THEN '<numero-contrato>' + CAST(FIN.NUFIN AS VARCHAR) + '</numero-contrato>' ELSE '' END,
        '<motivo-exclusao>',
        '<id>1</id>',
        '</motivo-exclusao>',
        '</excluir>',
        '</web:excluirSpc>',
        '</soapenv:Body>',
        '</soapenv:Envelope>'
    ) AS XML_DATAEXC,
    FIN.CODPARC,
    UPPER(PAR.EMAIL) AS EMAIL,
    PAR.TELEFONE
FROM TGFFIN FIN
INNER JOIN TGFPAR PAR ON FIN.CODPARC = PAR.CODPARC
INNER JOIN TSIEND ED ON PAR.CODEND = ED.CODEND
INNER JOIN TSIBAI BAI ON PAR.CODBAI = BAI.CODBAI
WHERE  FIN.NUFIN = :P_NUFIN
ORDER BY 1 ASC