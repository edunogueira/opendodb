import asyncio
import aiohttp

class LoginManager:
    def __init__(self):
        self.username = ""
        self.password = ""
        self.session_cookie = None

    async def login(self):
        """Faz o login e retorna o cookie PHPSESSID."""
        login_url = "https://www.dugout-online.com/home/none/Free-online-football-manager-game"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.dugout-online.com/",
            "Referer": "https://www.dugout-online.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0",
            "sec-ch-ua": '"Chromium";v="130", "Opera";v="115", "Not?A_Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        data = {
            "attemptLogin": "1",
            "do_user": self.username,
            "do_pass": self.password,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(login_url, headers=headers, data=data) as response:
                if response.status == 200:
                    # Obtém o cookie da sessão
                    cookies = session.cookie_jar.filter_cookies("https://www.dugout-online.com")
                    php_session_cookie = cookies.get("PHPSESSID")
                    if php_session_cookie:
                        self.session_cookie = php_session_cookie.value
                        return self.session_cookie
                    else:
                        raise Exception("Cookie PHPSESSID não encontrado na resposta.")
                else:
                    raise Exception(f"Falha no login. Status code: {response.status}")

    def get_session_cookie(self):
        """Retorna o cookie PHPSESSID obtido após o login."""
        if self.session_cookie:
            return self.session_cookie
        else:
            raise Exception("Nenhum cookie disponível. Faça o login primeiro.")
