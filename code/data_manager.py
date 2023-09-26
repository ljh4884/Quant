import pandas as pd
import warnings
import requests
import bs4
import os

warnings.simplefilter("ignore")


class DataManager:
    base_df = None

    def load_base_df(self):
        if not self.base_df:
            self.base_df = pd.read_excel("../data/base_info.xlsx")[['한글 종목명', '단축코드']]
        return self.base_df

    def update_all(self):
        if not self.base_df:
            self.base_df = self.load_base_df()
        for i in range(len(self.base_df)):
            info_df = self.base_df.loc[i]
            name = info_df['한글 종목명']
            code = info_df['단축코드']

            print(i, name, code)

            base_path = "../data/stock_csv/"
            file_name = name + "_" + code + ".csv"

            file_path = base_path + file_name
            bs = bs4.BeautifulSoup
            df_list = []
            if os.path.isfile(file_path):
                old_df = pd.read_csv(file_path)
                last_day = old_df.iloc[0]['날짜']
                for page_id in range(1, 1000):
                    url = f"https://finance.naver.com/item/sise_day.nhn?code={code}&page={page_id}"

                    headers = {
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                                            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'
                    }

                    response = requests.get(url, headers=headers)
                    parsing_response = bs(response.text, "html.parser")
                    df_info = pd.read_html(str(parsing_response))
                    df = df_info[0].dropna().reset_index(drop=True)

                    condition_df = df[df['날짜'] == last_day]
                    if not condition_df.empty:
                        index = condition_df.index[0]
                        if index > 0:
                            df = df.iloc[:index]
                            df_list.append(df)
                        break

                    df_list.append(df)
                if len(df_list) > 0:
                    new_df = pd.concat(df_list, ignore_index=True)
                    new_df = pd.concat([new_df, old_df], ignore_index=True)
                    new_df.to_csv(file_path)


            else:
                for page_id in range(1, 1000):
                    url = f"https://finance.naver.com/item/sise_day.nhn?code={code}&page={page_id}"

                    headers = {
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'
                    }

                    response = requests.get(url, headers=headers)
                    parsing_response = bs(response.text, "html.parser")
                    df_info = pd.read_html(str(parsing_response))
                    df = df_info[0].dropna().reset_index(drop=True)
                    info = list(df_info[1].iloc[0])
                    last_page = info[len(info) - 1]
                    df_list.append(df)
                    if str(page_id) == str(last_page):
                        break
                new_df = pd.concat(df_list, ignore_index=True)
                new_df.to_csv(file_path)




