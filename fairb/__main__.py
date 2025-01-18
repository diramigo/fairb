import argparse
import sys
from fairb.scripts import create, design, run, submit, merge

def main():
    parser = argparse.ArgumentParser(
        description="CLI para ejecutar scripts en mi_paquete."
    )
    parser.add_argument(
        "script", choices=["create", "design", "run", "submit", "merge"], help="El script a ejecutar"
    )
    parser.add_argument(
        "args", nargs=argparse.REMAINDER, help="Argumentos para el script seleccionado"
    )

    args = parser.parse_args()

    if args.script == "create":
        create.main(args.args)
    elif args.script == "design":
        design.main(args.args)
    elif args.script == "run":
        run.main(args.args)
    elif args.script == "submit":
        submit.main(args.args)
    elif args.script == "merge":
        merge.main(args.args)

if __name__ == "__main__":
    main()
