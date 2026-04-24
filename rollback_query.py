from query_version_manager import QueryVersionManager


def main():
    manager = QueryVersionManager()
    canal = input("Informe o canal para rollback: ").strip()

    resultado = manager.rollback_ultima_versao(canal)
    print(resultado)


if __name__ == "__main__":
    main()