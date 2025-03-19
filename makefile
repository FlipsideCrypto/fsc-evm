new_repo_tag:
	@echo "Last 3 tags:"
	@git tag -l --sort=-v:refname | head -n 3
	@echo ""
	@read -p "Enter new tag name (e.g., v1.1.0) or 'q' to quit: " tag_name; \
	if [ "$$tag_name" = "q" ]; then \
		echo "Operation cancelled."; \
		exit 0; \
	elif [ -n "$$tag_name" ]; then \
		if git diff-index --quiet HEAD --; then \
			echo "No changes to commit. Proceeding with tagging."; \
		else \
			git add . && \
			git commit -m "Prepare release $$tag_name" && \
			git push; \
		fi; \
		if git push --dry-run 2>&1 | grep -q "Everything up-to-date"; then \
			echo "Remote is up-to-date. Skipping push."; \
		else \
			git push; \
		fi; \
		git tag -a $$tag_name -m "version $$tag_name" && \
		git push origin --tags && \
		echo "Tag $$tag_name created and pushed successfully."; \
	else \
		echo "No tag name entered. Operation cancelled."; \
	fi
copy-selectors:
	@if [ -f dbt_packages/fsc_evm/selectors.yml ]; then \
		cp dbt_packages/fsc_evm/selectors.yml ./selectors.yml && \
		echo "Successfully copied selectors.yml to project root"; \
	else \
		echo "Error: dbt_packages/fsc_evm/selectors.yml not found"; \
		exit 1; \
	fi
append-selectors:
	@if [ -f dbt_packages/fsc_evm/selectors.yml ] && [ -f selectors.yml ]; then \
		echo "Merging selectors..."; \
		awk 'BEGIN {p=1} \
			/^selectors:/ {if(p==1) {print; p=0; next}} \
			p==1 {print} \
			/^selectors:/ {p=0; next} \
			p==0 {print}' selectors.yml > selectors_temp.yml && \
		awk '/^selectors:/ {next} {print}' dbt_packages/fsc_evm/selectors.yml >> selectors_temp.yml && \
		mv selectors_temp.yml selectors.yml && \
		echo "Successfully merged selectors"; \
	else \
		echo "Error: One or both selector files not found"; \
		exit 1; \
	fi
.PHONY: new_repo_tag copy-selectors append-selectors