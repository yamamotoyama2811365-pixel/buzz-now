import unittest
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime, timezone
from app import detective_media, detective_posts, social_mix, social_schedule

ROOT = Path(__file__).resolve().parents[1]


class DetectiveMediaTest(unittest.TestCase):
    def test_image_is_verified(self):
        info = detective_media.inspect(ROOT)
        self.assertTrue(info['ready'])
        self.assertEqual((info['width'], info['height']), (640, 800))
        self.assertFalse(info['runtime_generation'])

    def test_both_platforms_use_identical_approved_photo(self):
        self.assertEqual(detective_posts.APPROVED_ASSET, detective_media.ASSET)
        self.assertEqual(social_mix.APPROVED_ASSET, detective_media.ASSET)
        self.assertEqual(detective_posts.media(0, {'start': datetime.now(timezone.utc)}), detective_media.ASSET)

    def test_truncated_or_placeholder_file_cannot_be_sent(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            image = root / detective_media.ASSET
            image.parent.mkdir(parents=True)
            shutil.copyfile(ROOT / detective_media.MANIFEST, root / detective_media.MANIFEST)
            image.write_text('/mnt/data/detective-approved.jpg')
            self.assertFalse(detective_media.inspect(root)['ready'])
            image.write_bytes((ROOT / detective_media.ASSET).read_bytes()[:-10])
            self.assertFalse(detective_media.inspect(root)['ready'])

    def test_missing_asset_not_ready(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertFalse(detective_media.inspect(folder)['ready'])

    def test_every_caption_uses_requested_labels(self):
        for day in range(14):
            for hour in (12, 21):
                text = social_schedule.character_text(day, {'start':datetime(2026,9,11,hour,tzinfo=timezone.utc)})
                self.assertTrue(text.startswith('🕵️ SNS捜査官｜BUZZ NOW\n\n'))
                self.assertNotIn('公式AIキャラクター', text)
                self.assertNotIn('#AIキャラクター', text)
                self.assertTrue(text.endswith('#SNS捜査官'))
                self.assertLessEqual(len(text)*2, 280)


if __name__ == '__main__': unittest.main()
