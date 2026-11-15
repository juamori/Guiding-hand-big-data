import verificar_segredos


def test_repositorio_sem_segredos():
    assert verificar_segredos.varrer() == []


def test_scanner_detecta_token(tmp_path):
    falso = "abcdefghij" * 3  # montado em tempo de execução para não acionar o próprio scanner
    (tmp_path / "x.py").write_text("api_" + 'key = "' + falso + '"\n')
    assert verificar_segredos.varrer(tmp_path)
