from query_version_manager import QueryVersionManager


def main():
    manager = QueryVersionManager()

    canal = input("Informe o canal da query: ").strip()
    print("\nCole a query corrigida abaixo.")
    print("Quando terminar, digite apenas FIM em uma nova linha.\n")

    linhas = []
    while True:
        linha = input()
        if linha.strip().upper() == "FIM":
            break
        linhas.append(linha)

    query_corrigida = "\n".join(linhas)

    observacao = input("\nObservação da correção: ").strip()

    caminho = manager.salvar_query_corrigida(
        canal=canal,
        query_corrigida=query_corrigida,
        origem_correcao="manual_com_backup",
        observacao=observacao,
    )

    print(f"\nQuery salva com backup em: {caminho}")


if __name__ == "__main__":
    main()