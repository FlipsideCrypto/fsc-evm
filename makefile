.PHONY: new_repo_tag

new_repo_tag:
	@echo "Last 3 tags:"
	@git tag -l --sort=-v:refname | head -n 3
	@echo ""
	@read -p "Enter new tag name (e.g., v1.1.0) or 'q' to quit: " tag_name; \
	if [ "$$tag_name" = "q" ]; then \
		echo "Operation cancelled."; \
		exit 0; \
	elif [ -n "$$tag_name" ]; then \
		git add . && \
		git commit -m "Prepare release $$tag_name" && \
		git push && \
		git tag -a $$tag_name -m "version $$tag_name" && \
		git push origin --tags && \
		echo "Tag $$tag_name created and pushed successfully."; \
	else \
		echo "No tag name entered. Operation cancelled."; \
	fi