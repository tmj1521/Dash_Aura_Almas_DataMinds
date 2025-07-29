import base64
import os

# Lista de imagens com variáveis desejadas
imagens = {
    "logo_aura": "Icones/Logo_Aura.jpg",
    "logo_mina": "Icones/caminhao.png",
    "logo_moagem": "Icones/mill.png",
    "logo_kpi": "Icones/kpi2.png"
}

# Arquivo de saída
saida = "imagens_base64.py"

with open(saida, "w", encoding="utf-8") as f_out:
    f_out.write("# Este arquivo foi gerado automaticamente com imagens em base64\n\n")
    for nome_variavel, caminho_imagem in imagens.items():
        ext = os.path.splitext(caminho_imagem)[1].lower()
        if ext not in [".jpg", ".jpeg", ".png"]:
            print(f"Formato não suportado: {caminho_imagem}")
            continue
        
        mime = "image/jpeg" if ext in [".jpg", ".jpeg"] else "image/png"
        with open(caminho_imagem, "rb") as img_file:
            b64 = base64.b64encode(img_file.read()).decode("utf-8")
            data_uri = f"data:{mime};base64,{b64}"
            f_out.write(f'{nome_variavel} = """{data_uri}"""\n\n')

print(f"✅ Imagens convertidas e salvas em {saida}")