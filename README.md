# README for EVM dbt macros, models and documentation

[EVM Wiki & Documentation](https://github.com/FlipsideCrypto/fsc-evm/wiki)

---

## Adding the `fsc_evm` dbt package

The `fsc_evm` dbt package is a centralized repository consisting of various dbt models, macros and snowflake functions that can be utilized across EVM repos.

1. Navigate to `packages.yml` in your respective repo.
2. Add the following (reference the latest version from [here](https://github.com/FlipsideCrypto/fsc-evm/tags)):
```
- git: https://github.com/FlipsideCrypto/fsc-evm.git
  revision: "v1.1.0"
```
3. Run `dbt clean && dbt deps` to install the package

**Troubleshooting:**
If `package_lock.yml` is present, you may need to remove it and re-run `dbt deps`. This is a known issue when installing dbt packages with the same version or revision tag.
  * If `makefile` is present, you can utilize the `make cleanup_time` command to clean and redeploy the packages.

---

## Recommended Development Flow

The recommended development flow for making changes to `fsc-evm` is as follows:

1. Create a new branch in `fsc-evm` with your changes (e.g. `AN-1234/dummy-branch`). When ready to test in another project, push your branch to the repository.
2. In your project (e.g. `swell-models`), update the version in `packages.yml` to your branch `revision: "AN-1234/dummy-branch"`.
3. Run `make cleanup_time` to pull in the current remote version of your branch.
   - This will delete `package-lock.yml` and run `dbt clean && dbt deps`.
4. Begin testing changes in project repository.
5. If more changes are needed to the `fsc-evm` branch:
   - Make sure to push them up and re-run `make cleanup_time` in the project.
   - Note: If you do not delete `package-lock.yml`, you likely won't pull in your latest changes, even if you run `dbt clean && dbt deps`.
6. Once the `fsc-evm` PR is ready, proceed to the [Adding Release Versions](#adding-release-versions) section.

---

## Adding Release Versions

1. First get PR approval/review before proceeding with version tagging.
2. Make the necessary changes to your code in your dbt package repository (e.g., `fsc-evm`).
3. Commit your changes with `git add .` and `git commit -m "Your commit message"`.
4. Push your commits to the remote repository with `git push ...`.
5. Tag your commit with a version number using `git tag -a v1.1.0 -m "version 1.1.0"`.
6. Push your tags to the remote repository with `git push origin --tags`.
7. Add official `Release` notes to the repo with the new tag.
  * Each `Release` should be formatted with the following template:
    ```
    Release Title: <vx.y.z release title>
    - Description of changes
    - ...

    **Full Changelog**: <link to the commits included in this new version> (hint: click the "Generate Release Notes" button on the release page to automatically generate this link)
    ```
8. In the `packages.yml` file of your other dbt project, specify the new version of the package with:

Alternatively, you can use the `makefile` to create a new tag and push it to the remote repository:

```
make new_repo_tag
```
```
Last 3 tags:
v1.11.0
v1.10.0
v1.9.0

Enter new tag name (e.g., v1.1.0) or 'q' to quit:
```

```
vx.y.z # where x, y, and z are the new version numbers (or q to quit)
```

### Version Strategy

- **Major versions** (v4.x.x → v5.x.x): Breaking changes, new features
- **Minor versions** (v4.1.x → v4.2.x): New features, backward compatible
- **Patch versions** (v4.1.1 → v4.1.2): Bug fixes, backward compatible

### Regarding Semantic Versioning;
1. Semantic versioning is a versioning scheme for software that aims to convey meaning about the underlying changes with each new release.
2. It's typically formatted as MAJOR.MINOR.PATCH (e.g. v1.2.3), where:
- MAJOR version (first number) should increment when there are potential breaking or incompatible changes that are structural to the design of the package.
- MINOR version (second number) should increment when functionality or features are added in a backwards-compatible manner or minor breaking changes, including those that require changes to variable names or table refreshes.
- PATCH version (third number) should increment when bug fixes are made without adding new features, or existing variables are updated.
3. Semantic versioning helps package users understand the degree of changes in a new release, and decide when to adopt new versions. With dbt packages, when you tag a release with a semantic version, users can specify the exact version they want to use in their projects.

---

### DBT Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices