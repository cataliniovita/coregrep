"""Clone repositoires to local storage"""
import subprocess
import json

from git import Repo

class CloneRepositories:
    def __init__(self, repositories_file, directory_location):
        self.repositories_file = repositories_file
        self.directory_location = directory_location

    def clone_repositories(self):
        """Clone all the repositories from the fetch_github_repos
        result script"""
        urls = self.parse_fetch_script()

        for url in urls:
            self.shallow_clone(url)


    def parse_fetch_script(self):
        """Parse the fetching script and populate the urls array
        with the html_url correspoding to every repository"""
        urls = []

        with open(self.repositories_file, "r") as repo_file:
            for line in repo_file:
                data = json.load(line)
                for key, value in data.items():
                    if "html_url" in key:
                        urls.append(value)

        return urls


    def shallow_clone(self, url):
        """Uses --depth=1 to create shallow clones and avoid
        big storage. Also use --filter=blob:none to avoid big files"""
        try:
            # subprocess.run(["git", "clone", "--depth=1", "--filter=blob:none", url])
            Repo.clone_from(url, self.directory_location)
        except Exception:
            print(f"[-] {url} couldn't be cloned!")


if __name__ == "__main__":
    clone_repository = CloneRepositories("repos_over_50000.jsonl", "/repositories")