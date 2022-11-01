import argparse
import subprocess


class ExtractRecipesFromRecipeListFileAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        recipeFiles = getattr(namespace, self.dest) or []
        for fileListFileName in values:
            with open(fileListFileName) as fileListFile:
                for line in fileListFile:
                    recipeFiles.append(line.strip())
        setattr(namespace, self.dest, recipeFiles)


parser = argparse.ArgumentParser(description='Script allows to sequentially execute multiple ingesters')
parser.register('action', 'ExtractRecipesFromRecipeListFileAction', ExtractRecipesFromRecipeListFileAction)

parser.add_argument('-l', '--recipe-list-file',
                    action='ExtractRecipesFromRecipeListFileAction',
                    nargs='*',
                    dest="recipeFiles",
                    help="file which contains list of recipe file names/paths (one name per one line)",
                    default=[])

parser.add_argument('-r', '--recipe-file',
                    action="extend",
                    nargs='*',
                    dest="recipeFiles",
                    help="recipe file name/path",
                    default=[])

args = parser.parse_args()

if len(args.recipeFiles) > 0:
    print("--- Final recipes list:", args.recipeFiles)

    for recipeFile in args.recipeFiles:
        print("--- Executing recipe: '$recipeFile'", recipeFile)
        # subprocess.run(["", "ingest -c", recipeFile])
        subprocess.run(["/datahub-src/metadata-ingestion/venv/bin/datahub", "ingest", "-c", recipeFile])
        print("--- /Executing recipe: '$recipeFile'", recipeFile, "succeeded")
else:
    print("--- No recipes provided")
    print()
    parser.print_help();
