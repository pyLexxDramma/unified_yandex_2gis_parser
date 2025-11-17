import re
import os


def count_and_remove_comments(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    original_lines_count = len(lines)
    cleaned_lines = []
    comments_removed = 0

    for line in lines:
        stripped_line = line.strip()

        if stripped_line.startswith('#'):
            comments_removed += 1
            continue
        else:
            cleaned_lines.append(line)

    final_lines_count = len(cleaned_lines)
    changes_made = original_lines_count - final_lines_count

    print(f"{file_path}: {comments_removed} комментариев удалены.")

    with open(file_path, 'w', encoding='utf-8') as file:
        file.writelines(cleaned_lines)

    return {
        'file': file_path,
        'comments_removed': comments_removed,
        'changes_made': changes_made
    }


def main():
    directory = input("Введите путь к папке с файлами (*.py): ")
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.py')]

    results = []
    total_comments_removed = 0
    total_changes_made = 0

    for file in files:
        result = count_and_remove_comments(file)
        results.append(result)
        total_comments_removed += result['comments_removed']
        total_changes_made += result['changes_made']

    print("\nИтоговая статистика:")
    for res in results:
        print(f"- Файл: {res['file']}, удалено {res['comments_removed']} комментариев")

    print(f"\nВсего удалено комментариев: {total_comments_removed}")
    print(f"Всего произведено изменений: {total_changes_made}\n")


if __name__ == "__main__":
    main()
