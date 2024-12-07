package com.craftaro.core.nms.v1_21_R3.nbt;

import com.craftaro.core.nms.nbt.NBTEntity;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntitySpawnReason;
import net.minecraft.world.entity.EntityType;
import org.bukkit.Location;
import org.bukkit.craftbukkit.v1_21_R3.CraftWorld;

import java.util.Optional;

public class NBTEntityImpl extends NBTCompoundImpl implements NBTEntity {
    private Entity nmsEntity;

    public NBTEntityImpl(CompoundTag entityNBT, Entity nmsEntity) {
        super(entityNBT);

        this.nmsEntity = nmsEntity;
    }

    @Override
    public org.bukkit.entity.Entity spawn(Location location) {
        String entityType = getNBTObject("entity_type").asString();
        getKeys().remove("UUID");

        Optional<EntityType<?>> optionalEntity = EntityType.byString(entityType);
        if (optionalEntity.isPresent()) {
            assert location.getWorld() != null;

            Entity spawned = optionalEntity.get().spawn(
                    ((CraftWorld) location.getWorld()).getHandle(),
                    null,
                    new BlockPos(location.getBlockX(), location.getBlockY(), location.getBlockZ()),
                    EntitySpawnReason.COMMAND,
                    true,
                    false
            );

            if (spawned != null) {
                spawned.load(this.compound);
                org.bukkit.entity.Entity entity = spawned.getBukkitEntity();
                entity.teleport(location);
                this.nmsEntity = spawned;

                return entity;
            }
        }

        return null;
    }

    @Override
    public org.bukkit.entity.Entity reSpawn(Location location) {
        this.nmsEntity.discard();
        return spawn(location);
    }

    @Override
    public void addExtras() {
        this.compound.putString("entity_type", BuiltInRegistries.ENTITY_TYPE.getKey(this.nmsEntity.getType()).toString());
    }
}
