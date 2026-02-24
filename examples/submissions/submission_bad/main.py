"""異常な提出プログラム（出力JSON形式が不正）

意図的に不正なJSON形式を出力するサンプル。
テスト用に、JSONではなくプレーンテキストを出力する。
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description="不正出力サンプル")
    parser.add_argument("--employee-master", required=True)
    parser.add_argument("--project-allocation", required=True)
    parser.add_argument("--working-hours", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    # 不正な出力: JSON形式ではなくプレーンテキストを書き込む
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("これはJSONではありません\n")
        f.write("分析結果: 残業多い人がいます\n")
        f.write("以上")

    print("処理完了（不正な形式で出力）")


if __name__ == "__main__":
    main()
