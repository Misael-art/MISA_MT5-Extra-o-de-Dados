"""Wine temporário de uma sessão visual; nunca reinicia o servidor da ponte.

O C: privado é o próprio perfil portátil. Registro e arquivos de sistema são
cópias; somente Common/Files é compartilhado para os canais UUID já existentes.
"""
import os
from pathlib import Path
import shutil
import subprocess


class Runtime:
    def __init__(self, source, directory):
        self.source, self.directory = Path(source), Path(directory)
        self.prefix = self.directory / ".wine"
        self.created = []
        self.desktop_size = None

    def prepare(self, copy_file):
        # Os nomes pertencem ao runtime, não aos artefatos do Expert.
        for name in (".wine", "windows", "users"):
            target = self.directory / name
            target.mkdir(mode=0o700, exist_ok=False)
            self.created.append(target)
        (self.prefix / "drive_c").symlink_to("..", target_is_directory=True)
        (self.prefix / "dosdevices").mkdir()
        (self.prefix / "dosdevices" / "c:").symlink_to("../drive_c", target_is_directory=True)
        (self.prefix / "dosdevices" / "z:").symlink_to("/", target_is_directory=True)
        for name in ("system.reg", "user.reg", "userdef.reg", ".update-timestamp"):
            copy_file(self.source / name, self.prefix / name)
        shutil.copytree(self.source / "drive_c" / "windows", self.directory / "windows",
                        dirs_exist_ok=True, copy_function=copy_file)
        for user in (self.source / "drive_c" / "users").iterdir():
            common = user / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"
            if common.is_dir():
                target = self.directory / "users" / user.name / common.relative_to(user)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(common.resolve(), target_is_directory=True)

    def configure_desktop(self, width, height):
        """Antes do primeiro Wine: modo interno da cópia igual ao monitor escolhido.

        Wine 11.17 enumera modos virtuais pela entrada Desktops/Default, mesmo
        quando explorer recebe um nome UUID. Sem ela, limita o MT5 à resolução
        do monitor primário; ampliar a janela X11 externa não corrige essa área.
        """
        if any(type(value) is not int or value <= 0 for value in (width, height)):
            raise ValueError("Dimensões do monitor devem ser inteiros positivos.")
        if self.prefix not in self.created:
            raise RuntimeError("O desktop precisa de uma cópia Wine privada preparada.")
        if self.desktop_size is not None:
            if self.desktop_size != (width, height):
                raise RuntimeError("O tamanho do monitor mudou durante a preparação. Inicie a sessão novamente no monitor desejado.")
            return  # não editar o registro de um servidor Wine já iniciado
        target = self.prefix / "user.reg"
        section = r"[Software\\Wine\\Explorer\\Desktops]"
        output, inside, found = [], False, False
        for line in target.read_text(encoding="utf-8").splitlines(keepends=True):
            if line.startswith("["):
                inside = line.startswith(section)
                output.append(line)
                if inside and not found:
                    output.append(f'"Default"="{width}x{height}"\n')
                    found = True
            elif not (inside and line.startswith('"Default"=')):
                output.append(line)
        if not found:
            output.append(f'\n{section}\n"Default"="{width}x{height}"\n')
        target.write_text("".join(output), encoding="utf-8")
        self.desktop_size = width, height

    def stop(self):
        """Somente o servidor deste prefixo UUID, incluindo serviços sem janela."""
        if self.prefix not in self.created:
            return  # preparação não adquiriu este caminho; não sinalizar terceiros
        env = dict(os.environ, WINEPREFIX=str(self.prefix), WINEDEBUG="-all")
        # -k retorna1 quando nenhum servidor chegou a iniciar ou já terminou.
        # A prova de encerramento é -w bem-sucedido, seguida da conferência
        # dos processos próprios pela Session; timeout/erro preserva as cópias.
        subprocess.run(["wineserver", "-k"], env=env, check=False, timeout=15,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["wineserver", "-w"], env=env, check=True, timeout=15,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def cleanup(self):
        # rmtree não segue drive_c/Common nem qualquer outro link de diretório.
        for target in reversed(self.created):
            shutil.rmtree(target)
        self.created.clear()
