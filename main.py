import aiohttp
import os
from pathlib import Path
from typing import List, Union, Dict, Any
from urllib.parse import urlparse
import asyncio

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import (
    Plain, Image, File, Node, Nodes, Forward
)
from astrbot.api.star import Context, Star, StarTools


class ForwardDownloadPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        custom_dir = config.get("save_path", "downloads")
        self.base_dir = StarTools.get_data_dir() / custom_dir
        self.debug = config.get("debug", False)

        # ---- 自动创建并检查目录可写性 ----
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            test_file = self.base_dir / ".write_test"
            test_file.touch()
            test_file.unlink()
            logger.info(f"保存目录已就绪: {self.base_dir.absolute()}")
        except Exception as e:
            logger.error(f"保存目录不可用: {self.base_dir.absolute()}, 错误: {e}")
            raise RuntimeError(f"无法创建或写入保存目录: {self.base_dir}") from e

        self.bot = None

    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def on_private_message(self, event: AstrMessageEvent):
        logger.info("准备处理私聊消息")
        self.bot = event.bot

        forward_comps = [
            c for c in event.message_obj.message
            if isinstance(c, (Forward, Node, Nodes))
        ]
        if not forward_comps:
            logger.info("不是转发消息，已忽略")
            return

        target_dir = self._next_available_dir()
        target_dir.mkdir(parents=True, exist_ok=True)

        results = []
        item_no = 1

        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for comp in forward_comps:
                desc = await self._process_component(
                    comp, target_dir, item_no, session, depth=1, is_top_level=True
                )
                results.append(desc)
                item_no += 1

        reply = f"已下载到：{target_dir}\n" + "\n".join(results[:20])
        yield event.plain_result(reply)

    # ===== 核心递归处理 =====
    async def _process_component(
        self,
        comp,
        parent_dir: Path,
        index: int,
        session: aiohttp.ClientSession,
        depth: int = 1,
        is_top_level: bool = False,
    ) -> str:
        if depth > 10:
            return f"{index}: 超过最大嵌套深度 (10)"

        if isinstance(comp, Forward):
            content = getattr(comp, 'content', None) or getattr(comp, 'nodes', None)
            if content:
                if isinstance(content, list):
                    descs = []
                    for i, item in enumerate(content, 1):
                        if isinstance(item, dict):
                            chain = item.get('message') or item.get('content', [])
                            if chain:
                                desc = await self._process_chain(
                                    chain, parent_dir, i, session, depth + 1
                                )
                            else:
                                desc = f"{i}: 空消息"
                        else:
                            desc = await self._process_component(
                                item, parent_dir, i, session, depth + 1
                            )
                        descs.append(desc)
                    return f"{index}: 转发(已展开) -> {parent_dir}\n" + "\n".join(descs)
                else:
                    return await self._process_component(
                        content, parent_dir, index, session, depth + 1
                    )

            msg_id = getattr(comp, 'id', None)
            if not msg_id:
                return f"{index}: 缺少转发消息 ID"

            try:
                if self.debug:
                    logger.info(f"[depth={depth}] 获取转发消息 ID={msg_id}")
                result = await self._fetch_forward_with_retry(msg_id)
                messages = result.get('messages', [])
                if not messages:
                    return f"{index}: 转发消息为空"

                descs = []
                for i, node_data in enumerate(messages, 1):
                    chain = node_data.get('message') or node_data.get('content', [])
                    if chain:
                        desc = await self._process_chain(
                            chain, parent_dir, i, session, depth + 1
                        )
                        descs.append(desc)
                    else:
                        descs.append(f"{i}: 空消息")
                return f"{index}: 转发消息(通过ID) -> {parent_dir}\n" + "\n".join(descs)
            except Exception as e:
                logger.error(f"获取转发消息失败 (ID={msg_id}): {e}")
                mark_file = parent_dir / f"_failed_forward_{msg_id}.txt"
                mark_file.write_text(
                    f"获取转发消息失败，ID={msg_id}\n错误: {str(e)}",
                    encoding='utf-8'
                )
                return f"{index}: 获取转发消息失败 (ID={msg_id})，已标记"

        if isinstance(comp, Nodes):
            nodes = getattr(comp, "nodes", []) or getattr(comp, "content", [])
            if not nodes:
                return f"{index}: 空转发"
            descs = []
            for i, node in enumerate(nodes, 1):
                desc = await self._process_component(
                    node, parent_dir, i, session, depth + 1
                )
                descs.append(desc)
            return f"{index}: 合并转发(Nodes) -> {parent_dir}\n" + "\n".join(descs)

        if isinstance(comp, Node):
            content = getattr(comp, "content", []) or getattr(comp, "message", [])
            if not content:
                return f"{index}: 空节点"
            return await self._process_chain(content, parent_dir, index, session, depth + 1)

        return f"{index}: 不支持的组件类型 {type(comp).__name__}"

    # ===== 处理消息链（支持视频和音频） =====
    async def _process_chain(
        self,
        chain: List[Union[Dict, Any]],
        parent_dir: Path,
        index: int,
        session: aiohttp.ClientSession,
        depth: int = 1
    ) -> str:
        if not chain:
            return f"{index}: 空消息链"

        # 简单链：单个非转发元素直接保存为文件
        if len(chain) == 1:
            seg = chain[0]
            if isinstance(seg, dict) and seg.get('type') != 'forward':
                seg_type = seg.get('type')
                if seg_type in ('text', 'image', 'file', 'video', 'record', 'audio'):
                    return await self._save_single_from_dict(seg, parent_dir, index, session)
            if isinstance(seg, (Plain, Image, File)):
                return await self._save_single(seg, parent_dir, index, session)

        # 复杂链：创建子目录
        sub_dir = parent_dir / str(index)
        sub_dir.mkdir(exist_ok=True)
        descs = []

        for i, seg in enumerate(chain, 1):
            if isinstance(seg, dict):
                seg_type = seg.get('type')
                seg_data = seg.get('data', {})

                if seg_type == 'text':
                    path = sub_dir / f"{i}.txt"
                    path.write_text(seg_data.get('text', ''), encoding='utf-8')
                    descs.append(f"文本 -> {path.name}")

                elif seg_type == 'image':
                    url = seg_data.get('url') or seg_data.get('file')
                    if url:
                        path = await self._download_from_url(url, sub_dir / str(i), '.jpg', session)
                        descs.append(f"图片 -> {path.name}")
                    else:
                        descs.append(f"{i}: 图片缺少 URL")

                elif seg_type == 'file':
                    url = seg_data.get('url') or seg_data.get('file')
                    if url:
                        path = await self._download_from_url(url, sub_dir / str(i), '.bin', session)
                        descs.append(f"文件 -> {path.name}")
                    else:
                        descs.append(f"{i}: 文件缺少 URL")

                elif seg_type in ('video',):
                    url = seg_data.get('url') or seg_data.get('file')
                    if url:
                        path = await self._download_from_url(url, sub_dir / str(i), '.mp4', session)
                        descs.append(f"视频 -> {path.name}")
                    else:
                        descs.append(f"{i}: 视频缺少 URL")

                elif seg_type in ('record', 'audio'):
                    url = seg_data.get('url') or seg_data.get('file')
                    if url:
                        path = await self._download_from_url(url, sub_dir / str(i), '.amr', session)
                        descs.append(f"音频 -> {path.name}")
                    else:
                        descs.append(f"{i}: 音频缺少 URL")

                elif seg_type == 'forward':
                    inner_messages = seg_data.get('messages') or seg_data.get('content') or seg_data.get('nodes')
                    if inner_messages:
                        if self.debug:
                            logger.info(f"[depth={depth}] 嵌套转发已展开，消息数: {len(inner_messages)}")
                        fwd_sub_dir = sub_dir / str(i)
                        fwd_sub_dir.mkdir(exist_ok=True)
                        descs_inner = []
                        for j, msg in enumerate(inner_messages, 1):
                            if isinstance(msg, dict):
                                chain_inner = msg.get('message') or msg.get('content', [])
                            else:
                                chain_inner = getattr(msg, 'message', []) or getattr(msg, 'content', [])
                            if chain_inner:
                                desc = await self._process_chain(
                                    chain_inner, fwd_sub_dir, j, session, depth + 1
                                )
                                descs_inner.append(desc)
                            else:
                                descs_inner.append(f"{j}: 空消息")
                        descs.append(f"{i}: 嵌套转发(已展开)\n" + "\n".join(descs_inner))
                    else:
                        fwd_id = seg_data.get('id')
                        if fwd_id:
                            try:
                                if self.debug:
                                    logger.info(f"[depth={depth}] 尝试用 ID={fwd_id} 获取嵌套转发")
                                result = await self._fetch_forward_with_retry(fwd_id)
                                messages = result.get('messages', [])
                                if messages:
                                    fwd_sub_dir = sub_dir / str(i)
                                    fwd_sub_dir.mkdir(exist_ok=True)
                                    descs_inner = []
                                    for j, msg in enumerate(messages, 1):
                                        chain_inner = msg.get('message') or msg.get('content', [])
                                        if chain_inner:
                                            desc = await self._process_chain(
                                                chain_inner, fwd_sub_dir, j, session, depth + 1
                                            )
                                            descs_inner.append(desc)
                                        else:
                                            descs_inner.append(f"{j}: 空消息")
                                    descs.append(f"{i}: 嵌套转发(通过ID)\n" + "\n".join(descs_inner))
                                else:
                                    descs.append(f"{i}: 嵌套转发获取为空 (ID={fwd_id})")
                            except Exception as e:
                                logger.warning(f"嵌套转发获取失败 (ID={fwd_id}): {e}")
                                mark_file = sub_dir / f"_unresolved_forward_{i}.txt"
                                mark_file.write_text(f"嵌套转发 ID={fwd_id} 无法解析: {e}", encoding='utf-8')
                                descs.append(f"{i}: 嵌套转发获取失败 (ID={fwd_id})")
                        else:
                            descs.append(f"{i}: 嵌套转发缺少 ID 和展开数据")

                else:
                    descs.append(f"{i}: 忽略类型 {seg_type}")

            elif isinstance(seg, (Plain, Image, File)):
                desc = await self._save_single(seg, sub_dir, i, session)
                descs.append(desc)
            else:
                descs.append(f"{i}: 未知对象类型 {type(seg).__name__}")

        return f"{index}: 消息链 -> {sub_dir}\n" + "\n".join(descs)

    # ===== 从字典直接保存单个元素（支持视频、音频） =====
    async def _save_single_from_dict(
        self,
        seg: dict,
        parent_dir: Path,
        index: int,
        session: aiohttp.ClientSession
    ) -> str:
        seg_type = seg.get('type')
        seg_data = seg.get('data', {})
        if seg_type == 'text':
            path = parent_dir / f"{index}.txt"
            path.write_text(seg_data.get('text', ''), encoding='utf-8')
            return f"{index}: 文本 -> {path}"
        elif seg_type == 'image':
            url = seg_data.get('url') or seg_data.get('file')
            if url:
                path = await self._download_from_url(url, parent_dir / str(index), '.jpg', session)
                return f"{index}: 图片 -> {path}"
            else:
                return f"{index}: 图片缺少 URL"
        elif seg_type == 'file':
            url = seg_data.get('url') or seg_data.get('file')
            if url:
                path = await self._download_from_url(url, parent_dir / str(index), '.bin', session)
                return f"{index}: 文件 -> {path}"
            else:
                return f"{index}: 文件缺少 URL"
        elif seg_type == 'video':
            url = seg_data.get('url') or seg_data.get('file')
            if url:
                path = await self._download_from_url(url, parent_dir / str(index), '.mp4', session)
                return f"{index}: 视频 -> {path}"
            else:
                return f"{index}: 视频缺少 URL"
        elif seg_type in ('record', 'audio'):
            url = seg_data.get('url') or seg_data.get('file')
            if url:
                path = await self._download_from_url(url, parent_dir / str(index), '.amr', session)
                return f"{index}: 音频 -> {path}"
            else:
                return f"{index}: 音频缺少 URL"
        else:
            return f"{index}: 不支持的类型 {seg_type}"

    # ===== 保存单个组件对象 =====
    async def _save_single(
        self,
        comp,
        parent_dir: Path,
        index: int,
        session: aiohttp.ClientSession
    ) -> str:
        if isinstance(comp, Plain):
            path = parent_dir / f"{index}.txt"
            path.write_text(comp.text, encoding='utf-8')
            return f"{index}: 文本 -> {path}"
        elif isinstance(comp, Image):
            url = getattr(comp, 'url', '') or getattr(comp, 'file', '')
            if not url:
                return f"{index}: 图片缺少资源地址"
            path = await self._download_from_url(
                url, parent_dir / str(index), '.jpg', session
            )
            return f"{index}: 图片 -> {path}"
        elif isinstance(comp, File):
            url = getattr(comp, 'url', '') or getattr(comp, 'file', '')
            if not url:
                return f"{index}: 文件缺少资源地址"
            path = await self._download_from_url(
                url, parent_dir / str(index), '.bin', session
            )
            return f"{index}: 文件 -> {path}"
        else:
            return f"{index}: 不支持的类型 ({type(comp).__name__})"

    # ===== 获取转发消息（含重试） =====
    async def _fetch_forward_with_retry(self, msg_id: str, retries: int = 2) -> Dict:
        for attempt in range(retries + 1):
            try:
                result = await self.bot.api.call_action('get_forward_msg', message_id=msg_id)
                if result and result.get('messages'):
                    return result
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                if attempt == retries:
                    raise
                logger.warning(f"获取转发消息 (ID={msg_id}) 失败 (尝试 {attempt+1}/{retries+1}): {e}")
                await asyncio.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"获取转发消息失败，ID={msg_id}")

    # ===== 下载工具 =====
    async def _download_from_url(
        self,
        url: str,
        target_path: Path,
        default_ext: str,
        session: aiohttp.ClientSession
    ) -> Path:
        ext = default_ext
        if url.startswith("http"):
            parsed = urlparse(url)
            base = os.path.basename(parsed.path)
            if "." in base:
                ext = os.path.splitext(base)[1] or default_ext
        final_path = Path(str(target_path) + ext)

        for attempt in range(3):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    content = await resp.read()
                final_path.write_bytes(content)
                if final_path.exists():
                    size = final_path.stat().st_size
                    logger.info(f"下载成功: {final_path} (大小: {size} 字节)")
                else:
                    logger.error(f"写入后文件不存在: {final_path}")
                return final_path
            except Exception as e:
                if attempt == 2:
                    logger.error(f"下载失败 (URL={url})，已重试3次: {e}")
                    raise
                logger.warning(f"下载失败 (URL={url})，重试 {attempt+1}: {e}")
                await asyncio.sleep(0.5 * (attempt + 1))
        return final_path

    # ===== 目录分配 =====
    def _next_available_dir(self) -> Path:
        i = 1
        while (self.base_dir / str(i)).exists():
            i += 1
        return self.base_dir / str(i)