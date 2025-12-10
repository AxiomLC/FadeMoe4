# Tool Testing Document
Created: December 10, 2025
Updated: December 10, 2025 - Added complete tool analysis

## Summary
This file was created to test file creation and editing workflow in the Void environment.

## Tool Sequence Mastered
1. `ls_dir` - Directory listing ✓ (must provide URI path)
2. `read_file` - File reading (essential before editing) ✓
3. `edit_file` - Precise file editing (must have exact matches) ✓
4. `create_file_or_folder` - File creation ✓
5. `rewrite_file` - Adding content to new file ✓

## Tools Available but Not Tested:
- `search_for_files` - Content-based file search
- `search_in_file` - Find patterns within a file
- `search_pathnames_only` - File name pattern search
- `run_command` - Terminal command execution
- `get_dir_tree` - Recursive directory tree
- `delete_file_or_folder` - File removal (with caution)

## Rules Observed
1. **Major revisions**: Include name/version/date/edit description as top comment
2. **Small edits**: Post snippet for copy/paste OR await instruction to apply directly
3. **Memory alert**: Notify if early chat history is fading
4. **Read before edit**: Always use `read_file` first to get exact current state
5. **Path format**: Use Windows paths (C:\Users\...) or relative from workspace
6. **Exact matches**: SEARCH blocks must be verbatim - no partial matches allowed
7. **URI parameter**: `ls_dir` requires URI parameter (cannot be empty)

## Critical Insight
The most common error with `edit_file` is using outdated ORIGINAL content. Always:
1. Use `read_file` to get current content
2. Copy-paste exact lines from the result
3. Keep SEARCH block minimal but uniquely identifiable
4. Test with non-critical files first

## Workflow Cheat Sheet
```markdown
Need to edit a file?

1. Read: `read_file` to get exact content  
2. Copy: Use exact lines from read output
3. Edit: Create SEARCH/REPLACE blocks
4. Apply: Use `edit_file` tool
5. Verify: Check the file again if needed
```

Finding a file?

1. List: `ls_dir` for directory overview
2. Search: `search_pathnames_only` for pattern matching
3. Content: `search_for_files` for text within files
4. Exact: `search_in_file` for specific file content